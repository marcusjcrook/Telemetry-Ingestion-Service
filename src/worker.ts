import { Pool } from 'pg';
import Redis from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

// Flush when the batch reaches this size or has waited for the timeout below.
const BATCH_SIZE_LIMIT = 1000;
const FLUSH_TIMEOUT_MS = 1000;

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  throw new Error('DATABASE_URL must be set');
}

const pgPool = new Pool({
  connectionString: DATABASE_URL,
  max: 10, // Worker process only needs a small connection pool.
});

const cache = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

interface TelemetryRecord {
  deviceId: string;
  metric: string;
  value: number;
  receivedAt: string;
}

/**
 * Checks if a value is a valid TelemetryRecord.
 */
function isTelemetryRecord(value: unknown): value is TelemetryRecord {
  return (
    typeof value === 'object' &&
    value !== null &&
    'deviceId' in value &&
    typeof value.deviceId === 'string' &&
    'metric' in value &&
    typeof value.metric === 'string' &&
    'value' in value &&
    typeof value.value === 'number' &&
    Number.isFinite(value.value) &&
    'receivedAt' in value &&
    typeof value.receivedAt === 'string'
  );
}

// In-memory buffer for batching telemetry records before flushing to PostgreSQL.
let batchBuffer: TelemetryRecord[] = [];
// Timer and promise for managing flush scheduling and serialization.
let flushTimer: NodeJS.Timeout | null = null;
// Promise that resolves when the current flush operation completes, or null if no flush is in progress.
let flushPromise: Promise<void> | null = null;
let shuttingDown = false;
// Promise that resolves when the worker loop has exited, used for graceful shutdown.
let workerPromise: Promise<void>;

/**
 * Schedules a flush operation if one is not already pending and the buffer is not empty.
 */
function scheduleFlush(): void {
  // Keep at most one timer pending so low-volume batches are flushed promptly.
  if (flushTimer || batchBuffer.length === 0) return;

  // Schedule a flush after the timeout to allow more items to accumulate in the batch.
  flushTimer = setTimeout(() => {
    flushTimer = null;
    void flushToDatabase();
  }, FLUSH_TIMEOUT_MS);
}

/**
 * Executes a high-performance batch insert into PostgreSQL using an unnested structural layout
 */
async function flushBatch(): Promise<void> {
  // Clear the timer if flushing early due to batch size limit
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }

  // Copy the current batch to avoid mutation during async operations.
  const currentBatch = [...batchBuffer];
  // Release the buffer before awaiting PostgreSQL so new queue items can accumulate.
  batchBuffer = []; // Immediately reset buffer to accept new items concurrently

  console.log(`Flushing ${currentBatch.length} metrics to PostgreSQL...`);

  try {
    // Construct an optimized multi-row insert query
    // e.g., INSERT INTO table (col1, col2) VALUES (\$1, \$2), (\$3, \$4)...
    const valuePlaceholders: string[] = [];
    const queryValues: (string | number)[] = [];
    let placeholderIndex = 1;

    currentBatch.forEach((item) => {
      valuePlaceholders.push(`($${placeholderIndex}, $${placeholderIndex + 1}, $${placeholderIndex + 2}, $${placeholderIndex + 3})`);
      queryValues.push(item.deviceId, item.metric, item.value, item.receivedAt);
      placeholderIndex += 4;
    });

    const queryText = `
      INSERT INTO device_telemetry (device_id, metric, value, received_at)
      VALUES ${valuePlaceholders.join(', ')}
    `;

    const startTime = performance.now();
    await pgPool.query(queryText, queryValues);
    const duration = performance.now() - startTime;

    console.log(`Batch insert complete. Took ${duration.toFixed(2)}ms`);
  } catch (error) {
    console.error('Failed to flush batch to database:', error);
    try {
      // Restore the original order because the worker consumes the list from its right side.
      await cache.rpush('telemetry_queue', ...currentBatch.reverse().map((item) => JSON.stringify(item)));
      console.warn(`Requeued ${currentBatch.length} metrics after the database insert failed.`);
    } catch (requeueError) {
      batchBuffer = [...currentBatch.reverse(), ...batchBuffer];
      console.error('Failed to requeue batch; retaining it in memory for retry:', requeueError);
    }
  }
}

/**
 * Flushes the in-memory batch buffer to the database.
 */
function flushToDatabase(): Promise<void> {
  // Serialize writes so only one PostgreSQL batch insert runs at a time.
  if (flushPromise) return flushPromise;
  if (batchBuffer.length === 0) return Promise.resolve();

  // Start the flush operation and clear the promise when done, then check if more flushing is needed.
  flushPromise = flushBatch().finally(() => {
    flushPromise = null;
    if (shuttingDown) return;

    if (batchBuffer.length >= BATCH_SIZE_LIMIT) {
      void flushToDatabase();
    } else {
      scheduleFlush();
    }
  });

  return flushPromise;
}

/**
 * Pushes items into memory buffer and schedules/triggers flushing
 */
function handleIncomingItem(itemString: string) {
  try {
    const item: unknown = JSON.parse(itemString);
    if (!isTelemetryRecord(item)) {
      console.error('Malformed queue message dropped: invalid telemetry record');
      return;
    }

    batchBuffer.push(item);

    // Flush immediately at capacity; otherwise the timer handles low-volume batches.
    if (batchBuffer.length >= BATCH_SIZE_LIMIT) {
      flushToDatabase();
      return;
    }

    // Schedule a flush if one is not already pending.
    scheduleFlush();
  } catch (err) {
    console.error('Malformed queue message dropped:', err);
  }
}

/**
 * Block on the Redis-compatible queue instead of polling while it is empty.
 */
async function startWorker() {
  console.log('Background batch worker active and listening for telemetry events...');

  while (!shuttingDown) {
    try {
      // BRPOP waits up to five seconds for an item in the Valkey-backed queue.
      const result = await cache.brpop('telemetry_queue', 5);
     
      if (result) {
        // The second tuple element contains the serialized telemetry record.
        handleIncomingItem(result[1]);
      }
    } catch (error) {
      console.error('Queue connection or pop error:', error);
      // Avoid repeatedly logging and retrying at full speed during a connection outage.
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

// Graceful shutdown handling
async function shutdown(): Promise<void> {
  console.log('\nShutdown signal received. Flushing remaining data...');
  // Stop consuming first, then let any active insert finish before draining the buffer.
  shuttingDown = true;
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  await workerPromise;
  if (flushPromise) await flushPromise;
  await flushToDatabase();
  if (batchBuffer.length > 0) {
    console.error('Worker shutdown with telemetry still buffered in memory; data may be lost.');
    process.exitCode = 1;
  }
  await cache.quit();
  await pgPool.end();
  console.log('Worker safely stopped.');
}

// Handle termination signals for graceful shutdown.
process.once('SIGINT', () => {
  void shutdown().catch((error: unknown) => {
    console.error('Worker shutdown failed:', error);
    process.exitCode = 1;
  });
});

// Handle termination signals for graceful shutdown.
process.once('SIGTERM', () => {
  void shutdown().catch((error: unknown) => {
    console.error('Worker shutdown failed:', error);
    process.exitCode = 1;
  });
});

workerPromise = startWorker();