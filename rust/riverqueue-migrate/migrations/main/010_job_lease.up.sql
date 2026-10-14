-- Store a lease only while a job is running.
ALTER TABLE /* TEMPLATE: schema */river_job
    ADD COLUMN lease_expires_at timestamptz,
    ADD CONSTRAINT river_job_lease_state_valid CHECK (lease_expires_at IS NULL OR state = 'running') NOT VALID;

-- Support rescuer scans independently of business execution duration.
CREATE INDEX river_job_lease_expiry_idx
    ON /* TEMPLATE: schema */river_job (lease_expires_at, id)
    WHERE state = 'running' AND lease_expires_at IS NOT NULL;
