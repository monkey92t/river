-- Remove the execution lease after leased workers have been drained.
DROP INDEX /* TEMPLATE: schema */river_job_lease_expiry_idx;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN lease_expires_at;
