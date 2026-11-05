# Telemetry Ingestion Service

A local telemetry ingestion app built with Express, PostgreSQL, and Valkey (Redis-compatible). The API queues telemetry in Valkey, and a background worker batches queued records into PostgreSQL.

## Prerequisites

- Docker Desktop with its engine running
- Node.js and npm
- k6, if you want to run the load test

## Start the app

Run these commands from the project directory in separate PowerShell terminals.

1. Create your local environment file from the example:

   ```powershell
   Copy-Item .env.example .env
   ```

   The example values are for local development only. Change the passwords in `.env` before use; `.env` is ignored by Git.

2. Start PostgreSQL, Valkey, and pgAdmin:

   ```powershell
   docker compose up -d
   ```

3. Install Node dependencies (first run, or after dependency changes):

   ```powershell
   npm install
   ```

4. Apply the database schema once. Open pgAdmin at [http://localhost:8080](http://localhost:8080), sign in with `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` from `.env`, and register the database server:

   - **Host:** `postgres` (pgAdmin runs inside Docker)
   - **Port:** `5432`
   - **Maintenance database:** `POSTGRES_DB` from `.env`
   - **Username/password:** `POSTGRES_USER` and `POSTGRES_PASSWORD` from `.env`

   Select the registered server and database, open **Tools → Query Tool**, and execute the contents of [`db/schema.sql`](./db/schema.sql). The schema creates `device_telemetry`, adds the device/time index, and migrates an existing table whose `metric` column is numeric. Existing numeric metrics are retained as `value`; their metric name is set to `unknown` because it cannot be inferred.

5. Start the API:

   ```powershell
   npm run dev:api
   ```

   The API listens at `http://localhost:3000`. Its health endpoint is `http://localhost:3000/health`.

6. Start the queue worker in another terminal:

   ```powershell
   npm run dev:worker
   ```

Keep both API and worker terminals open while using the app.
The API and worker read `DATABASE_URL` and `REDIS_URL` from `.env`. If you change the PostgreSQL credentials, database name, or mapped port there, update `DATABASE_URL` to match.

The worker requeues a batch in Valkey if its PostgreSQL insert fails. If Valkey is also unavailable, it retains the batch in memory and retries; stopping the worker before that batch can be requeued may still lose those records. A database timeout can also make it unclear whether a batch committed before the failure was reported, so retries may create duplicate rows. Valkey uses `noeviction` so it rejects writes when full instead of evicting queued telemetry.

## Send telemetry

The endpoint is `POST /api/v1/telemetry`. A valid JSON body includes a device ID, metric name, and numeric value:

```json
{
  "deviceId": "device-001",
  "metric": "temperature",
  "value": 21.5
}
```

The API responds with HTTP `202` when the record has been added to the queue. The worker persists it to PostgreSQL asynchronously.

## Run the k6 test

With Docker services, API, and worker running, execute:

```powershell
k6 run .\telemetry-test.js
```

The test checks that requests receive HTTP `202` and measures API response times. Confirm persistence in pgAdmin with:

```sql
SELECT id, device_id, metric, value, received_at, processed_at
FROM device_telemetry
ORDER BY received_at DESC
LIMIT 10;
```

## Stop the app

- In the API terminal, press **Ctrl+C**.
- In the worker terminal, press **Ctrl+C** so it can flush its current batch.
- Stop the Docker services from the project directory:

  ```powershell
  docker compose down
  ```

This preserves the database and Valkey data volumes. Do not add `-v` unless you intend to delete the persisted data.

## Local credentials

The values in `.env.example` are placeholders for local development only. Change them in `.env`; do not reuse them in production or commit `.env`. The Docker services bind to localhost by default. Adjust the `*_PORT` variables in `.env` if those ports are already in use.
