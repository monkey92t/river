-- Store a lease only while a job is running.
ALTER TABLE /* TEMPLATE: schema */river_job
    ADD COLUMN lease_expires_at timestamp CHECK (lease_expires_at IS NULL OR state = 'running');

-- Support rescuer scans independently of business execution duration.
CREATE INDEX /* TEMPLATE: schema */river_job_lease_expiry_idx
    ON river_job (lease_expires_at, id)
    WHERE state = 'running' AND lease_expires_at IS NOT NULL;
