package index_unused

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

// TestIndexUnused_WithUnusedIndexes verifies that index:unused correctly
// detects indexes with zero or low scan counts
func TestIndexUnused_WithUnusedIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with unused indexes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with indexes that won't be used
	setupSQL := `
		CREATE TABLE archived_data (
			id SERIAL PRIMARY KEY,
			legacy_id VARCHAR(100),
			old_status VARCHAR(50),
			archived_at TIMESTAMP
		);
		
		-- Create indexes that will remain unused
		CREATE INDEX idx_archived_legacy ON archived_data(legacy_id);
		CREATE INDEX idx_archived_status ON archived_data(old_status);
		
		INSERT INTO archived_data (legacy_id, old_status, archived_at) 
		SELECT 
			'LEG-' || generate_series,
			'archived',
			NOW() - INTERVAL '1 year'
		FROM generate_series(1, 100);
		
		ANALYZE archived_data;
		
		-- Query using primary key only (not the other indexes)
		SELECT * FROM archived_data WHERE id = 1;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the index:unused command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--scan-count-max", "0",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should contain unused index information
	assert.Contains(t, output, "unused indexes")
	assert.Contains(t, output, "archived_data")
	assert.Contains(t, output, "Scans")
}

// TestIndexUnused_JSONOutput verifies that index:unused produces
// valid JSON output with correct structure
func TestIndexUnused_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with unused indexes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE temp_logs (
			id SERIAL PRIMARY KEY,
			log_level VARCHAR(20),
			message TEXT
		);
		
		CREATE INDEX idx_temp_level ON temp_logs(log_level);
		
		INSERT INTO temp_logs (log_level, message) 
		SELECT 
			CASE WHEN generate_series % 3 = 0 THEN 'ERROR' ELSE 'INFO' END,
			'Log message ' || generate_series
		FROM generate_series(1, 100);
		
		ANALYZE temp_logs;
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
		"--scan-count-max", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON
	var results []unusedIndexRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// Verify JSON structure if results exist
	if len(results) > 0 {
		assert.NotEmpty(t, results[0].Schema)
		assert.NotEmpty(t, results[0].Table)
		assert.NotEmpty(t, results[0].Index)
		assert.GreaterOrEqual(t, results[0].Scans, int64(0))
	}
}

// TestIndexUnused_ExcludesPrimaryKey verifies that index:unused
// correctly excludes primary key indexes
func TestIndexUnused_ExcludesPrimaryKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with tables having primary keys
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE test_entities (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100)
		);
		
		INSERT INTO test_entities (name) 
		SELECT 'Entity ' || generate_series 
		FROM generate_series(1, 50);
		
		ANALYZE test_entities;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the index:unused command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--scan-count-max", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should not include primary key indexes
	var results []unusedIndexRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.NotContains(t, row.Index, "_pkey",
			"Primary key indexes should be excluded")
	}
}

// TestIndexUnused_ScanCountMaxFilter verifies that --scan-count-max filter
// correctly includes only indexes below the threshold
func TestIndexUnused_ScanCountMaxFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with indexes having various scan counts
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE activity_log (
			id SERIAL PRIMARY KEY,
			action_type VARCHAR(50),
			user_id INTEGER,
			created_at TIMESTAMP DEFAULT NOW()
		);
		
		CREATE INDEX idx_activity_type ON activity_log(action_type);
		CREATE INDEX idx_activity_user ON activity_log(user_id);
		CREATE INDEX idx_activity_created ON activity_log(created_at);
		
		INSERT INTO activity_log (action_type, user_id) 
		SELECT 
			CASE WHEN generate_series % 2 = 0 THEN 'login' ELSE 'logout' END,
			generate_series % 20
		FROM generate_series(1, 200);
		
		ANALYZE activity_log;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with scan-count-max filter set to 5
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--scan-count-max", "5",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only include indexes with <= 5 scans
	var results []unusedIndexRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.LessOrEqual(t, row.Scans, int64(5),
			"All results should have scan count <= 5")
	}
}

// TestIndexUnused_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestIndexUnused_SchemaFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with indexes in different schemas
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE SCHEMA legacy;
		
		CREATE TABLE public.current_data (
			id SERIAL PRIMARY KEY,
			value INTEGER
		);
		
		CREATE TABLE legacy.old_data (
			id SERIAL PRIMARY KEY,
			value INTEGER
		);
		
		CREATE INDEX idx_current_value ON public.current_data(value);
		CREATE INDEX idx_old_value ON legacy.old_data(value);
		
		INSERT INTO public.current_data (value) 
		SELECT generate_series FROM generate_series(1, 50);
		
		INSERT INTO legacy.old_data (value) 
		SELECT generate_series FROM generate_series(1, 50);
		
		ANALYZE public.current_data;
		ANALYZE legacy.old_data;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with schema filter for legacy
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "legacy",
		"--scan-count-max", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain legacy schema
	var results []unusedIndexRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "legacy", row.Schema,
			"All results should be from legacy schema")
	}
}

// TestIndexUnused_Explain verifies that --explain flag prints
// explanation without executing the query
func TestIndexUnused_Explain(t *testing.T) {
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
	assert.Contains(t, output, "overhead")
	assert.Contains(t, output, "DROP")
}
