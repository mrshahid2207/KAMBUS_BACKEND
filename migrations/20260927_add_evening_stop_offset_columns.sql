-- Run once against the KAMBUS database before persisting evening stop offsets.
-- DOUBLE PRECISION is accepted by PostgreSQL and SQLite.
ALTER TABLE stops ADD COLUMN evening_latitude DOUBLE PRECISION;
ALTER TABLE stops ADD COLUMN evening_longitude DOUBLE PRECISION;
