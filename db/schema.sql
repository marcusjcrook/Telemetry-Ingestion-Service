CREATE TABLE IF NOT EXISTS device_telemetry (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    metric VARCHAR(100) NOT NULL,
    value NUMERIC NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'device_telemetry'
          AND column_name = 'metric'
          AND data_type IN ('numeric', 'integer', 'bigint', 'real', 'double precision')
    ) THEN
        ALTER TABLE device_telemetry RENAME COLUMN metric TO value;
        ALTER TABLE device_telemetry ADD COLUMN metric VARCHAR(100);
        UPDATE device_telemetry SET metric = 'unknown' WHERE metric IS NULL;
        ALTER TABLE device_telemetry ALTER COLUMN metric SET NOT NULL;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
    ON device_telemetry (device_id, received_at DESC);
