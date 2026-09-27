CREATE TABLE admin_bus_changes (
    id SERIAL PRIMARY KEY,
    source_bus_id INTEGER NOT NULL REFERENCES buses(id),
    target_bus_id INTEGER NOT NULL REFERENCES buses(id),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_by INTEGER NOT NULL REFERENCES users(id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    announcement_id INTEGER REFERENCES announcement_history(id)
);

CREATE INDEX ix_admin_bus_changes_source_bus_id ON admin_bus_changes (source_bus_id);
CREATE INDEX ix_admin_bus_changes_start_date ON admin_bus_changes (start_date);
CREATE INDEX ix_admin_bus_changes_end_date ON admin_bus_changes (end_date);
CREATE INDEX ix_admin_bus_changes_status ON admin_bus_changes (status);
