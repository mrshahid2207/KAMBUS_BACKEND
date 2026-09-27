ALTER TABLE temporary_stop_changes
    ADD COLUMN IF NOT EXISTS morning_temporary_stop_id INTEGER REFERENCES stops(id);

ALTER TABLE temporary_stop_changes
    ADD COLUMN IF NOT EXISTS evening_temporary_stop_id INTEGER REFERENCES stops(id);

UPDATE temporary_stop_changes
SET morning_temporary_stop_id = temporary_stop_id
WHERE morning_temporary_stop_id IS NULL;

UPDATE temporary_stop_changes
SET evening_temporary_stop_id = temporary_stop_id
WHERE evening_temporary_stop_id IS NULL;
