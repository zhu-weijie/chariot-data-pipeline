CREATE SCHEMA IF NOT EXISTS jobs;

CREATE TABLE IF NOT EXISTS jobs.aggregation_batches (
    batch_id SERIAL PRIMARY KEY,
    start_movie_id INT NOT NULL,
    end_movie_id INT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aggregation_batches_status ON jobs.aggregation_batches(status);

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_aggregation_batches_modtime
BEFORE UPDATE ON jobs.aggregation_batches
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();
