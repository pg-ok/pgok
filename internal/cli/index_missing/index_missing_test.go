package index_missing

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

// TestIndexMissing_WithHighSeqScans verifies that index:missing correctly
// detects tables with high sequential scan ratios
func TestIndexMissing_WithHighSeqScans(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with a large table without appropriate indexes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with large table
	setupSQL := `
		CREATE TABLE large_orders (
			id SERIAL PRIMARY KEY,
			customer_id INTEGER NOT NULL,
			status VARCHAR(50) NOT NULL,
			amount DECIMAL(10, 2)
		);
		
		-- Insert substantial data to exceed rows-min threshold
		INSERT INTO large_orders (customer_id, status, amount) 
		SELECT 
			generate_series % 100,
			CASE WHEN generate_series % 3 = 0 THEN 'pending' 
			     WHEN generate_series % 3 = 1 THEN 'shipped' 
			     ELSE 'delivered' END,
			(random() * 1000)::DECIMAL(10, 2)
		FROM generate_series(1, 2000);
		
		ANALYZE large_orders;
		
		-- Force sequential scans by querying without index
		SELECT * FROM large_orders WHERE customer_id = 42;
		SELECT * FROM large_orders WHERE status = 'pending';
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the index:missing command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--rows-min", "1000",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should contain missing index information
	assert.Contains(t, output, "missing indexes")
	assert.Contains(t, output, "RATIO")
	assert.Contains(t, output, "SEQ SCANS")
}

// TestIndexMissing_JSONOutput verifies that index:missing produces
// valid JSON output with correct structure
func TestIndexMissing_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with tables having sequential scans
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE events (
			id SERIAL PRIMARY KEY,
			event_type VARCHAR(100) NOT NULL,
			event_date DATE NOT NULL
		);
		
		INSERT INTO events (event_type, event_date) 
		SELECT 
			'event_' || (generate_series % 10),
			CURRENT_DATE - (generate_series % 365)
		FROM generate_series(1, 1500);
		
		ANALYZE events;
		
		-- Trigger sequential scans
		SELECT * FROM events WHERE event_type = 'event_5';
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
		"--rows-min", "1000",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON
	var results []missingIndexRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// Verify JSON structure if results exist
	if len(results) > 0 {
		assert.NotEmpty(t, results[0].Schema)
		assert.NotEmpty(t, results[0].Table)
		assert.GreaterOrEqual(t, results[0].SequentialScans, int64(0))
		assert.GreaterOrEqual(t, results[0].IndexScans, int64(0))
		assert.GreaterOrEqual(t, results[0].TableRows, int64(0))
	}
}

// TestIndexMissing_RowsMinFilter verifies that --rows-min filter
// correctly excludes small tables
func TestIndexMissing_RowsMinFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with both small and large tables
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		-- Small table (below threshold)
		CREATE TABLE small_config (
			id SERIAL PRIMARY KEY,
			key VARCHAR(100),
			value VARCHAR(100)
		);
		
		-- Large table (above threshold)
		CREATE TABLE large_logs (
			id SERIAL PRIMARY KEY,
			message TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		);
		
		INSERT INTO small_config (key, value) 
		SELECT 'key_' || generate_series, 'value_' || generate_series 
		FROM generate_series(1, 50);
		
		INSERT INTO large_logs (message) 
		SELECT 'log message ' || generate_series 
		FROM generate_series(1, 1500);
		
		ANALYZE small_config;
		ANALYZE large_logs;
		
		-- Trigger sequential scans on both
		SELECT * FROM small_config WHERE key = 'key_10';
		SELECT * FROM large_logs WHERE message LIKE 'log%';
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with high rows-min threshold
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--rows-min", "1000",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only include large_logs
	var results []missingIndexRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.GreaterOrEqual(t, row.TableRows, int64(1000),
			"All results should have >= 1000 rows")
	}
}

// TestIndexMissing_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestIndexMissing_SchemaFilter(t *testing.T) {
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
		CREATE SCHEMA analytics;
		
		CREATE TABLE public.web_requests (
			id SERIAL PRIMARY KEY,
			url TEXT,
			status INTEGER
		);
		
		CREATE TABLE analytics.page_views (
			id SERIAL PRIMARY KEY,
			page TEXT,
			views INTEGER
		);
		
		INSERT INTO public.web_requests (url, status) 
		SELECT 'url_' || generate_series, 200 
		FROM generate_series(1, 1200);
		
		INSERT INTO analytics.page_views (page, views) 
		SELECT 'page_' || generate_series, generate_series * 10 
		FROM generate_series(1, 1200);
		
		ANALYZE public.web_requests;
		ANALYZE analytics.page_views;
		
		SELECT * FROM public.web_requests WHERE url = 'url_1';
		SELECT * FROM analytics.page_views WHERE page = 'page_1';
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with schema filter for analytics
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "analytics",
		"--rows-min", "1000",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain analytics schema
	var results []missingIndexRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "analytics", row.Schema,
			"All results should be from analytics schema")
	}
}

// TestIndexMissing_Explain verifies that --explain flag prints
// explanation without executing the query
func TestIndexMissing_Explain(t *testing.T) {
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
	assert.Contains(t, output, "Sequential Scan")
}
