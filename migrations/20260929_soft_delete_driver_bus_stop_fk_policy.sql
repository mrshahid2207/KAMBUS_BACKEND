-- Soft-delete/permanent-delete FK policy for PostgreSQL.
-- Run as one transaction against Render before enabling permanent-delete routes.
-- Nullable FK orphans are preserved as history by clearing only the broken link.

BEGIN;

ALTER TABLE drivers ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE stops ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE buses ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
UPDATE buses SET is_active = FALSE WHERE status = 'inactive';
UPDATE buses SET status = 'inactive' WHERE is_active = FALSE;

DO $$
DECLARE
    rec RECORD;
    existing_constraint TEXT;
    new_constraint_name TEXT;
    repaired_rows BIGINT;
    has_orphans BOOLEAN;
BEGIN
    FOR rec IN
        SELECT * FROM (VALUES
            ('students', 'bus_id', 'buses', 'SET NULL', TRUE),
            ('students', 'stop_id', 'stops', 'SET NULL', TRUE),
            ('buses', 'driver_id', 'drivers', 'SET NULL', TRUE),
            ('trips', 'bus_id', 'buses', 'SET NULL', TRUE),
            ('trips', 'driver_id', 'drivers', 'SET NULL', TRUE),
            ('driver_complaints', 'bus_id', 'buses', 'SET NULL', TRUE),
            ('driver_complaints', 'driver_id', 'drivers', 'SET NULL', TRUE),
            ('bus_entry_logs', 'bus_id', 'buses', 'SET NULL', TRUE),
            ('wait_requests', 'bus_id', 'buses', 'SET NULL', TRUE),
            ('wait_requests', 'stop_id', 'stops', 'SET NULL', TRUE),
            ('notifications', 'related_bus_id', 'buses', 'SET NULL', TRUE),
            ('notifications', 'user_id', 'users', 'SET NULL', TRUE),
            ('device_tokens', 'user_id', 'users', 'CASCADE', FALSE),
            ('admin_bus_changes', 'source_bus_id', 'buses', 'SET NULL', TRUE),
            ('admin_bus_changes', 'target_bus_id', 'buses', 'SET NULL', TRUE),
            ('missed_bus_allotments', 'original_bus_id', 'buses', 'SET NULL', TRUE),
            ('missed_bus_allotments', 'alternative_bus_id', 'buses', 'SET NULL', TRUE),
            ('missed_bus_allotments', 'stop_id', 'stops', 'SET NULL', TRUE),
            ('temporary_stop_changes', 'target_bus_id', 'buses', 'SET NULL', TRUE),
            ('temporary_stop_changes', 'original_stop_id', 'stops', 'SET NULL', TRUE),
            ('temporary_stop_changes', 'temporary_stop_id', 'stops', 'SET NULL', TRUE),
            ('temporary_stop_changes', 'morning_temporary_stop_id', 'stops', 'SET NULL', TRUE),
            ('temporary_stop_changes', 'evening_temporary_stop_id', 'stops', 'SET NULL', TRUE),
            ('bus_locations', 'bus_id', 'buses', 'CASCADE', FALSE)
        ) AS policy(table_name, column_name, referenced_table, delete_action, make_nullable)
    LOOP
        IF rec.make_nullable THEN
            EXECUTE format(
                'ALTER TABLE %I ALTER COLUMN %I DROP NOT NULL',
                rec.table_name,
                rec.column_name
            );

            EXECUTE format(
                'UPDATE %I AS t '
                || 'SET %I = NULL '
                || 'WHERE t.%I IS NOT NULL '
                || 'AND NOT EXISTS (SELECT 1 FROM %I AS p WHERE p.id = t.%I)',
                rec.table_name,
                rec.column_name,
                rec.column_name,
                rec.referenced_table,
                rec.column_name
            );
            GET DIAGNOSTICS repaired_rows = ROW_COUNT;
            RAISE NOTICE 'FK cleanup: %.% repaired % orphaned row(s)',
                rec.table_name, rec.column_name, repaired_rows;
        ELSE
            EXECUTE format(
                'SELECT EXISTS ('
                || 'SELECT 1 FROM %I AS t '
                || 'WHERE t.%I IS NOT NULL '
                || 'AND NOT EXISTS (SELECT 1 FROM %I AS p WHERE p.id = t.%I)'
                || ')',
                rec.table_name,
                rec.column_name,
                rec.referenced_table,
                rec.column_name
            ) INTO has_orphans;

            IF has_orphans THEN
                RAISE EXCEPTION
                    'Cannot add foreign key for %.%: orphaned values exist in a NOT NULL column',
                    rec.table_name,
                    rec.column_name;
            END IF;
        END IF;

        -- Remove legacy names and this migration's deterministic name on reruns.
        FOR existing_constraint IN
            SELECT kcu.constraint_name
            FROM information_schema.key_column_usage AS kcu
            JOIN information_schema.table_constraints AS tc
              ON tc.constraint_schema = kcu.constraint_schema
             AND tc.constraint_name = kcu.constraint_name
             AND tc.table_name = kcu.table_name
            JOIN pg_constraint AS pc
              ON pc.conname = kcu.constraint_name
             AND pc.conrelid = format('%I.%I', kcu.table_schema, kcu.table_name)::regclass
            WHERE kcu.table_schema = current_schema()
              AND kcu.table_name = rec.table_name
              AND kcu.column_name = rec.column_name
              AND tc.constraint_type = 'FOREIGN KEY'
              AND pc.confrelid = to_regclass(rec.referenced_table)
        LOOP
            EXECUTE format(
                'ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I',
                rec.table_name,
                existing_constraint
            );
        END LOOP;

        new_constraint_name := left(
            'fk_' || rec.table_name || '_' || rec.column_name || '_soft_delete',
            63
        );
        EXECUTE format(
            'ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I',
            rec.table_name,
            new_constraint_name
        );
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I(id) ON DELETE %s',
            rec.table_name,
            new_constraint_name,
            rec.column_name,
            rec.referenced_table,
            rec.delete_action
        );
    END LOOP;
END $$;

COMMIT;
