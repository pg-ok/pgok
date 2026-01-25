package table_missing_pk

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/pg-ok/pgok/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTableMissingPK_WithMissingPK verifies that table:missing-pk correctly
// detects tables without primary keys
func TestTableMissingPK_WithMissingPK(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with tables missing primary keys
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with table lacking primary key
	setupSQL := `
		CREATE TABLE audit_logs (
			log_id INTEGER,
			action VARCHAR(100),
			performed_at TIMESTAMP DEFAULT NOW(),
			user_name VARCHAR(100)
		);
		-- Note: No PRIMARY KEY constraint
		
		CREATE TABLE session_data (
			session_id VARCHAR(255),
			data TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		);
		-- Note: No PRIMARY KEY constraint
		
		INSERT INTO audit_logs (log_id, action, user_name) 
		SELECT 
			generate_series,
			'Action ' || generate_series,
			'User ' || (generate_series % 10)
		FROM generate_series(1, 100);
		
		INSERT INTO session_data (session_id, data) 
		SELECT 
			'SESSION-' || generate_series,
			'Data for session ' || generate_series
		FROM generate_series(1, 50);
		
		ANALYZE audit_logs;
		ANALYZE session_data;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the table:missing-pk command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should contain tables without primary keys
	assert.Contains(t, output, "tables without PRIMARY KEY")
	assert.Contains(t, output, "audit_logs")
	assert.Contains(t, output, "session_data")
}

// TestTableMissingPK_JSONOutput verifies that table:missing-pk produces
// valid JSON output with correct structure
func TestTableMissingPK_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with tables missing primary keys
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE temp_imports (
			import_id INTEGER,
			data TEXT,
			imported_at TIMESTAMP
		);
		
		INSERT INTO temp_imports (import_id, data, imported_at) 
		SELECT 
			generate_series,
			'Import data ' || generate_series,
			NOW()
		FROM generate_series(1, 75);
		
		ANALYZE temp_imports;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for JSON output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with JSON output format
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON
	var results []tableMissingPkRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// Verify JSON structure
	require.Greater(t, len(results), 0, "Should find tables without primary keys")
	assert.Equal(t, "public", results[0].Schema)
	assert.Equal(t, "temp_imports", results[0].Table)
	assert.NotEmpty(t, results[0].SizeHuman)
	assert.GreaterOrEqual(t, results[0].SizeBytes, int64(0))
}

// TestTableMissingPK_AllTablesHavePK verifies that table:missing-pk correctly
// reports when all tables have primary keys
func TestTableMissingPK_AllTablesHavePK(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database where all tables have primary keys
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE customers (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255)
		);
		
		CREATE TABLE orders (
			order_id SERIAL PRIMARY KEY,
			customer_id INTEGER,
			total DECIMAL(10, 2)
		);
		
		INSERT INTO customers (name, email) 
		SELECT 
			'Customer ' || generate_series,
			'customer' || generate_series || '@example.com'
		FROM generate_series(1, 40);
		
		INSERT INTO orders (customer_id, total) 
		SELECT 
			(generate_series % 40) + 1,
			(random() * 500)::DECIMAL(10, 2)
		FROM generate_series(1, 120);
		
		ANALYZE customers;
		ANALYZE orders;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the table:missing-pk command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: Output should indicate all tables have primary keys
	assert.Contains(t, output, "All tables have a Primary Key")
}

// TestTableMissingPK_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestTableMissingPK_SchemaFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with tables in different schemas
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE SCHEMA staging;
		
		-- Public schema table without PK
		CREATE TABLE public.raw_data (
			data_id INTEGER,
			content TEXT
		);
		
		-- Staging schema table without PK
		CREATE TABLE staging.imported_records (
			record_id INTEGER,
			value TEXT
		);
		
		INSERT INTO public.raw_data (data_id, content) 
		SELECT generate_series, 'Content ' || generate_series 
		FROM generate_series(1, 30);
		
		INSERT INTO staging.imported_records (record_id, value) 
		SELECT generate_series, 'Value ' || generate_series 
		FROM generate_series(1, 30);
		
		ANALYZE public.raw_data;
		ANALYZE staging.imported_records;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with schema filter for staging
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "staging",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain staging schema
	var results []tableMissingPkRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "staging", row.Schema,
			"All results should be from staging schema")
	}
}

// TestTableMissingPK_OrderedBySize verifies that results are ordered
// by size in descending order
func TestTableMissingPK_OrderedBySize(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with multiple tables without PKs of varying sizes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		-- Small table
		CREATE TABLE small_table (
			id INTEGER,
			name VARCHAR(50)
		);
		
		-- Medium table
		CREATE TABLE medium_table (
			id INTEGER,
			description TEXT
		);
		
		-- Large table
		CREATE TABLE large_table (
			id INTEGER,
			content TEXT
		);
		
		INSERT INTO small_table (id, name) 
		SELECT generate_series, 'Name ' || generate_series 
		FROM generate_series(1, 20);
		
		INSERT INTO medium_table (id, description) 
		SELECT generate_series, repeat('Desc ', 20) || generate_series 
		FROM generate_series(1, 100);
		
		INSERT INTO large_table (id, content) 
		SELECT generate_series, repeat('Content ', 50) || generate_series 
		FROM generate_series(1, 200);
		
		ANALYZE small_table;
		ANALYZE medium_table;
		ANALYZE large_table;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running table:missing-pk command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should be ordered by size descending
	var results []tableMissingPkRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	// Verify we have results
	require.Greater(t, len(results), 0)

	// Verify ordering (each subsequent table should be <= previous)
	for i := 1; i < len(results); i++ {
		assert.LessOrEqual(t, results[i].SizeBytes, results[i-1].SizeBytes,
			"Results should be ordered by size descending")
	}
}

// TestTableMissingPK_Explain verifies that --explain flag prints
// explanation without executing the query
func TestTableMissingPK_Explain(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: Valid database connection string
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with --explain flag
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--explain",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: Output should contain explanation text
	assert.Contains(t, output, "EXPLANATION")
	assert.Contains(t, output, "INTERPRETATION")
	assert.Contains(t, output, "SQL QUERY")
	assert.Contains(t, output, "Primary Key")
	assert.Contains(t, output, "replication")
}
