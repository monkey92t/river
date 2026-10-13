-- Refuse to remove the coordinator while any sequence data remains.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM /* TEMPLATE: schema */river_sequence_group)
       OR EXISTS (SELECT 1 FROM /* TEMPLATE: schema */river_job WHERE sequence_key IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot remove sequences while sequence groups or members remain';
    END IF;
END $$;

DROP TABLE /* TEMPLATE: schema */river_sequence_group;
ALTER TABLE /* TEMPLATE: schema */river_job
    DROP CONSTRAINT river_job_sequence_shape,
    DROP COLUMN sequence_key,
    DROP COLUMN sequence_position,
    DROP COLUMN sequence_continue_on_cancelled,
    DROP COLUMN sequence_continue_on_discarded,
    DROP COLUMN sequence_resolved_at;
