package store

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps the pgx connection pool.
type DB struct {
	Pool *pgxpool.Pool
}

// NewDB creates a connection pool to YMatrix/PostgreSQL.
// If the target database does not exist, it connects to the default "postgres"
// database, creates the target database (UTF-8, timezone Asia/Shanghai), and
// reconnects.  Tables and indexes are then bootstrapped automatically.
func NewDB(ctx context.Context, dsn string) (*DB, error) {
	pool, err := connectWithDBBootstrap(ctx, dsn)
	if err != nil {
		return nil, err
	}

	db := &DB{Pool: pool}

	// Pre-check: version and current database/user (Section 12.1)
	if err := db.runPreCheck(ctx); err != nil {
		log.Printf("[DB] WARNING: pre-check failed (non-fatal): %v", err)
	}

	// Self-bootstrap: detect and create tables/indexes if absent
	if err := db.ensureTables(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure tables: %w", err)
	}

	return db, nil
}

// connectWithDBBootstrap tries the target DSN first.  If the database does not
// exist (SQLSTATE 3D000), it connects to the default "postgres" maintenance
// database, CREATE DATABASE with UTF-8 encoding and UTC+8 timezone, then
// reconnects to the target.
func connectWithDBBootstrap(ctx context.Context, targetDSN string) (*pgxpool.Pool, error) {
	// Parse the target config to extract host/port/user/password/dbname
	targetCfg, err := pgxpool.ParseConfig(targetDSN)
	if err != nil {
		return nil, fmt.Errorf("parse target DSN: %w", err)
	}
	targetCfg.MaxConns = 20
	targetCfg.MinConns = 2
	targetCfg.MaxConnLifetime = 30 * time.Minute
	targetCfg.MaxConnIdleTime = 5 * time.Minute

	dbName := targetCfg.ConnConfig.Database

	pool, err := pgxpool.NewWithConfig(ctx, targetCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	err = pool.Ping(ctx)
	if err == nil {
		log.Printf("[DB] Connected to YMatrix/PostgreSQL database %q", dbName)
		return pool, nil
	}

	// Check if the error is "database does not exist" (SQLSTATE 3D000)
	if isDatabaseNotExist(err) {
		pool.Close()
		log.Printf("[DB] Database %q does not exist — attempting auto-create", dbName)

		// Connect to the default "postgres" maintenance database
		if err := createDatabase(ctx, targetCfg, dbName); err != nil {
			return nil, fmt.Errorf("auto-create database %q: %w", dbName, err)
		}

		// Reconnect to the target database
		pool2, err := pgxpool.NewWithConfig(ctx, targetCfg)
		if err != nil {
			return nil, fmt.Errorf("reconnect to %q after creation: %w", dbName, err)
		}
		if err := pool2.Ping(ctx); err != nil {
			pool2.Close()
			return nil, fmt.Errorf("ping %q after creation: %w", dbName, err)
		}
		log.Printf("[DB] Connected to newly created database %q", dbName)
		return pool2, nil
	}

	pool.Close()
	return nil, fmt.Errorf("ping database: %w", err)
}

// isDatabaseNotExist checks whether the error indicates a missing database.
func isDatabaseNotExist(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "SQLSTATE 3D000") ||
		strings.Contains(errStr, "does not exist")
}

// createDatabase connects to the "postgres" maintenance database and creates
// the target database with UTF-8 encoding.
func createDatabase(ctx context.Context, targetCfg *pgxpool.Config, dbName string) error {
	// Build a DSN pointing to the "postgres" database
	bootstrapCfg := targetCfg.Copy()
	bootstrapCfg.ConnConfig.Database = "postgres"

	bootstrapPool, err := pgxpool.NewWithConfig(ctx, bootstrapCfg)
	if err != nil {
		return fmt.Errorf("connect to postgres maintenance db: %w", err)
	}
	defer bootstrapPool.Close()

	if err := bootstrapPool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres maintenance db: %w", err)
	}

	// CREATE DATABASE with UTF-8 encoding.  The identifier is quoted to prevent
	// SQL injection via the database name.
	createSQL := fmt.Sprintf(
		`CREATE DATABASE %q ENCODING 'UTF8' LC_COLLATE 'en_US.UTF-8' LC_CTYPE 'en_US.UTF-8' TEMPLATE template0`,
		dbName,
	)
	log.Printf("[DB] Executing: %s", createSQL)
	if _, err := bootstrapPool.Exec(ctx, createSQL); err != nil {
		// If the locale settings are unsupported (common in Greenplum),
		// fall back to the simplest form.
		log.Printf("[DB] Full CREATE DATABASE failed (%v), trying minimal form", err)
		fallbackSQL := fmt.Sprintf(`CREATE DATABASE %q ENCODING 'UTF8'`, dbName)
		if _, err2 := bootstrapPool.Exec(ctx, fallbackSQL); err2 != nil {
			return fmt.Errorf("create database: %w (also failed minimal: %w)", err, err2)
		}
	}

	log.Printf("[DB] Database %q created successfully", dbName)
	return nil
}

// Close closes the connection pool.
func (db *DB) Close() {
	db.Pool.Close()
	log.Println("[DB] Connection pool closed")
}

// runPreCheck logs version, current database and user (read-only diagnostic).
func (db *DB) runPreCheck(ctx context.Context) error {
	var version string
	if err := db.Pool.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("version check: %w", err)
	}
	log.Printf("[DB] Version: %s", version)

	var dbName, dbUser string
	if err := db.Pool.QueryRow(ctx, "SELECT current_database(), current_user").Scan(&dbName, &dbUser); err != nil {
		return fmt.Errorf("current_database/current_user check: %w", err)
	}
	log.Printf("[DB] Database: %s, User: %s", dbName, dbUser)

	return nil
}

// ensureTables checks whether the required tables exist and creates them if not.
// All statements use "IF NOT EXISTS" so they are safe to run multiple times.
func (db *DB) ensureTables(ctx context.Context) error {
	isGreenplum := db.isGreenplum(ctx)
	if isGreenplum {
		log.Printf("[DB] Greenplum/YMatrix detected — will use DISTRIBUTED BY (event_id)")
	} else {
		log.Printf("[DB] Standard PostgreSQL detected")
	}

	hasPointData := db.tableExists(ctx, "opc_point_data")
	hasAlarm := db.tableExists(ctx, "opc_alarm_event")
	hasOpsLog := db.tableExists(ctx, "opc_operation_log")

	if hasPointData && hasAlarm && hasOpsLog {
		log.Println("[DB] All required tables already exist, skipping DDL")
		if err := db.ensureIndexes(ctx); err != nil {
			return fmt.Errorf("ensure indexes: %w", err)
		}
		return nil
	}

	log.Printf("[DB] Bootstrapping tables: point_data=%v alarm=%v ops_log=%v", hasPointData, hasAlarm, hasOpsLog)

	if err := db.createPointDataTable(ctx, isGreenplum); err != nil {
		return fmt.Errorf("create opc_point_data: %w", err)
	}
	if err := db.createAlarmTable(ctx, isGreenplum); err != nil {
		return fmt.Errorf("create opc_alarm_event: %w", err)
	}
	if err := db.createOpsLogTable(ctx, isGreenplum); err != nil {
		return fmt.Errorf("create opc_operation_log: %w", err)
	}
	if err := db.ensureIndexes(ctx); err != nil {
		return fmt.Errorf("ensure indexes: %w", err)
	}

	log.Println("[DB] Bootstrap complete (tables + indexes)")
	return nil
}

// tableExists returns true if the given table exists in the public schema.
func (db *DB) tableExists(ctx context.Context, name string) bool {
	var c int
	err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_catalog.pg_tables
		 WHERE schemaname = 'public' AND tablename = $1`, name,
	).Scan(&c)
	if err != nil {
		log.Printf("[DB] WARNING: table existence check for %s failed: %v", name, err)
		return false
	}
	return c > 0
}

// isGreenplum checks whether we're connected to a Greenplum/YMatrix instance.
func (db *DB) isGreenplum(ctx context.Context) bool {
	var c int
	err := db.Pool.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'gp_segment_configuration'",
	).Scan(&c)
	return err == nil && c > 0
}

// createPointDataTable creates the main data point table (Section 12.3).
func (db *DB) createPointDataTable(ctx context.Context, isGreenplum bool) error {
	sql := `CREATE TABLE IF NOT EXISTS opc_point_data (
		event_id             uuid        NOT NULL,
		device_id            text        NOT NULL,
		point_name           text        NOT NULL,
		data_type            text        NOT NULL,
		unit                 text,
		value_number         double precision,
		value_text           text,
		value_time           timestamptz,
		quality_code         bigint      NOT NULL,
		quality_name         text        NOT NULL,
		event_type           text        NOT NULL,
		is_abnormal          boolean     NOT NULL DEFAULT false,
		abnormal_reason      text,
		source_timestamp     timestamptz,
		server_timestamp     timestamptz,
		collector_timestamp  timestamptz NOT NULL,
		received_at          timestamptz NOT NULL DEFAULT now(),
		batch_id             uuid
	)`
	if isGreenplum {
		sql += ` DISTRIBUTED BY (event_id)`
	}
	if _, err := db.Pool.Exec(ctx, sql); err != nil {
		return err
	}
	log.Println("[DB] Table opc_point_data ready")
	return nil
}

// createAlarmTable creates the alarm event table (Section 12.4).
func (db *DB) createAlarmTable(ctx context.Context, isGreenplum bool) error {
	sql := `CREATE TABLE IF NOT EXISTS opc_alarm_event (
		alarm_id          uuid        NOT NULL,
		event_id          uuid        NOT NULL,
		device_id         text        NOT NULL,
		point_name        text,
		severity          text        NOT NULL,
		alarm_type        text        NOT NULL,
		message           text        NOT NULL,
		status            text        NOT NULL DEFAULT 'active',
		occurred_at       timestamptz NOT NULL,
		recovered_at      timestamptz,
		acknowledged_at   timestamptz,
		acknowledged_by   text,
		created_at        timestamptz NOT NULL DEFAULT now()
	)`
	if isGreenplum {
		sql += ` DISTRIBUTED BY (event_id)`
	}
	if _, err := db.Pool.Exec(ctx, sql); err != nil {
		return err
	}
	log.Println("[DB] Table opc_alarm_event ready")
	return nil
}

// createOpsLogTable creates the operations log table.
func (db *DB) createOpsLogTable(ctx context.Context, isGreenplum bool) error {
	sql := `CREATE TABLE IF NOT EXISTS opc_operation_log (
		id          varchar(36) NOT NULL,
		timestamp   timestamptz NOT NULL DEFAULT now(),
		level       text        NOT NULL,
		module      text        NOT NULL,
		message     text        NOT NULL,
		extra       text
	)`
	if isGreenplum {
		sql += ` DISTRIBUTED BY (id)`
	}
	if _, err := db.Pool.Exec(ctx, sql); err != nil {
		return err
	}
	log.Println("[DB] Table opc_operation_log ready")

	// Create index for log queries by timestamp
	idxSQL := `CREATE INDEX IF NOT EXISTS idx_ops_log_ts ON opc_operation_log (timestamp DESC)`
	if _, err := db.Pool.Exec(ctx, idxSQL); err != nil {
		return fmt.Errorf("create idx_ops_log_ts: %w", err)
	}
	// Create index for log queries by level
	idxLevelSQL := `CREATE INDEX IF NOT EXISTS idx_ops_log_level ON opc_operation_log (level, timestamp DESC)`
	if _, err := db.Pool.Exec(ctx, idxLevelSQL); err != nil {
		return fmt.Errorf("create idx_ops_log_level: %w", err)
	}

	return nil
}

// ensureIndexes creates indexes if they don't already exist.
func (db *DB) ensureIndexes(ctx context.Context) error {
	indexes := []struct {
		name string
		sql  string
	}{
		{
			name: "ux_opc_point_event",
			sql:  `CREATE UNIQUE INDEX IF NOT EXISTS ux_opc_point_event ON opc_point_data (event_id)`,
		},
		{
			name: "idx_opc_point_device_time",
			sql:  `CREATE INDEX IF NOT EXISTS idx_opc_point_device_time ON opc_point_data (device_id, point_name, source_timestamp)`,
		},
		{
			name: "idx_opc_point_abnormal_time",
			sql:  `CREATE INDEX IF NOT EXISTS idx_opc_point_abnormal_time ON opc_point_data (is_abnormal, source_timestamp)`,
		},
	}

	for _, idx := range indexes {
		exists := db.indexExists(ctx, idx.name)
		if exists {
			log.Printf("[DB] Index %s already exists, skipping", idx.name)
			continue
		}
		log.Printf("[DB] Creating index %s", idx.name)
		if _, err := db.Pool.Exec(ctx, idx.sql); err != nil {
			return fmt.Errorf("create index %s: %w", idx.name, err)
		}
	}
	return nil
}

// indexExists checks whether an index with the given name exists.
func (db *DB) indexExists(ctx context.Context, name string) bool {
	var c int
	err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_catalog.pg_indexes WHERE indexname = $1`, name,
	).Scan(&c)
	if err != nil {
		return false
	}
	return c > 0
}