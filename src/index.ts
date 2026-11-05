import express, { Request, Response } from 'express';
import { Pool } from 'pg';
import Redis from 'ioredis';
import dotenv from 'dotenv';

// Load environment variables from .env file.
dotenv.config();

// Create an Express application.
const app = express();
app.use(express.json());

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  throw new Error('DATABASE_URL must be set');
}
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const PORT = Number(process.env.PORT || 3000);

// Create a PostgreSQL connection pool with a maximum of 20 connections and appropriate timeouts.
const pgPool = new Pool({
  connectionString: DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Create a Redis client with lazy connection and retry settings.
const cache = new Redis(REDIS_URL, {
  lazyConnect: true,
  maxRetriesPerRequest: 3,
  enableOfflineQueue: false,
});

// Handle Redis connection events for logging and error handling.
cache.on('connect', () => {
  console.log('Connected to Redis');
});

cache.on('error', (error: Error) => {
  console.error('Redis connection error:', error.message);
});

// Health check endpoint to verify the status of Redis and PostgreSQL connections.
app.get('/health', async (_req: Request, res: Response): Promise<void> => {
  try {
    // Report queue readiness separately from the PostgreSQL connectivity check.
    const redisStatus = cache.status === 'ready' ? 'connected' : 'disconnected';
    const pgResult = await pgPool.query('SELECT 1');
    res.json({
      status: 'ok',
      redis: redisStatus,
      postgres: pgResult.rowCount !== null ? 'connected' : 'disconnected',
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown database error';
    res.status(503).json({
      status: 'degraded',
      redis: cache.status === 'ready' ? 'connected' : 'disconnected',
      postgres: 'disconnected',
      error: message,
    });
  }
});

// Telemetry ingestion endpoint.
app.post('/api/v1/telemetry', async (req: Request, res: Response): Promise<void> => {
  try {
    const payload = req.body;

    // Validate the payload structure before processing.
    if (
      !payload ||
      typeof payload !== 'object' ||
      typeof payload.deviceId !== 'string' ||
      payload.deviceId.length === 0 ||
      typeof payload.metric !== 'string' ||
      payload.metric.length === 0 ||
      typeof payload.value !== 'number' ||
      !Number.isFinite(payload.value)
    ) {
      res.status(400).json({ error: 'Invalid payload structure' });
      return;
    }

    // Check if Redis is connected before attempting to push to the queue
    if (cache.status !== 'ready') {
      res.status(503).json({ error: 'Telemetry queue unavailable; Redis is not connected' });
      return;
    }

    // Stamp the accepted payload before placing it on the durable queue.
    const serializedData = JSON.stringify({
      ...payload, // Spread the original payload properties.
      receivedAt: new Date().toISOString(), // Add the receivedAt timestamp.
    });

    // LPUSH pairs with the worker's BRPOP to consume records in arrival order.
    await cache.lpush('telemetry_queue', serializedData);
    res.status(202).json({ status: 'Accepted' });
  } catch (error) {
    console.error('Ingestion Error:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

/**
 * Starts the application and establishes connections to Redis and PostgreSQL.
 */
async function start(): Promise<void> {
  // Test Redis connection.
  try {
    await cache.connect();
    console.log('Redis connection established');
  } catch (error) {
    console.warn('Redis not available; telemetry ingestion will return 503 until Redis is running.');
    console.warn(error instanceof Error ? error.message : String(error));
  }

  // Test PostgreSQL connection.
  try {
    await pgPool.query('SELECT 1');
    console.log('PostgreSQL connection established');
  } catch (error) {
    console.warn('PostgreSQL not available; database health checks will fail until Postgres is running.');
    console.warn(error instanceof Error ? error.message : String(error));
  }

  // Start the Express server.
  app.listen(PORT, () => {
    console.log(`Ingestion engine listening on port ${PORT}`);
  });
}

// Start the application.
start().catch((error: unknown) => {
  console.error('Failed to start application:', error);
  process.exit(1);
});
