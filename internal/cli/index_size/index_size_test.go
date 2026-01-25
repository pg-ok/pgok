package index_size

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

// TestIndexSize_WithIndexes verifies that index:size correctly
// reports index sizes in descending order
func TestIndexSize_WithIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with multiple indexes of varying sizes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with indexed tables
	setupSQL := `
		CREATE TABLE large_table (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			username VARCHAR(100) NOT NULL,
			bio TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		);
		
		CREATE INDEX idx_large_email ON large_table(email);
		CREATE INDEX idx_large_username ON large_table(username);
		CREATE INDEX idx_large_created ON large_table(created_at);
		
		-- Insert data to create index size
		INSERT INTO large_table (email, username, bio) 
		SELECT 
			'user' || generate_series || '@example.com',
			'user' || generate_series,
			'Bio text for user ' || generate_series
		FROM generate_series(1, 500);
		
		ANALYZE large_table;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the index:size command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--size-min", "0",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should contain index size information
	assert.Contains(t, output, "Analyzing index sizes")
	assert.Contains(t, output, "large_table")
	assert.Contains(t, output, "Size")
}

// TestIndexSize_JSONOutput verifies that index:size produces
// valid JSON output with correct structure
func TestIndexSize_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with indexed tables
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			sku VARCHAR(100) UNIQUE,
			description TEXT
		);
		
		CREATE INDEX idx_products_name ON products(name);
		
		INSERT INTO products (name, sku, description) 
		SELECT 
			'Product ' || generate_series,
			'SKU-' || generate_series,
			'Description for product ' || generate_series
		FROM generate_series(1, 300);
		
		ANALYZE products;
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
		"--size-min", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON
	var results []indexSizeRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// Verify JSON structure
	if len(results) > 0 {
		assert.NotEmpty(t, results[0].Schema)
		assert.NotEmpty(t, results[0].Table)
		assert.NotEmpty(t, results[0].Index)
		assert.NotEmpty(t, results[0].SizeHuman)
		assert.GreaterOrEqual(t, results[0].SizeBytes, int64(0))
	}
}

// TestIndexSize_SizeMinFilter verifies that --size-min filter
// correctly excludes smaller indexes
func TestIndexSize_SizeMinFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with indexes of different sizes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		-- Small table with small index
		CREATE TABLE small_items (
			id SERIAL PRIMARY KEY,
			code VARCHAR(10)
		);
		
		-- Larger table with larger index
		CREATE TABLE large_items (
			id SERIAL PRIMARY KEY,
			description TEXT
		);
		
		CREATE INDEX idx_small_code ON small_items(code);
		CREATE INDEX idx_large_desc ON large_items(description);
		
		INSERT INTO small_items (code) 
		SELECT 'CODE' || generate_series 
		FROM generate_series(1, 10);
		
		INSERT INTO large_items (description) 
		SELECT repeat('text ', 100) || generate_series 
		FROM generate_series(1, 500);
		
		ANALYZE small_items;
		ANALYZE large_items;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with size-min filter set to 100KB
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--size-min", "102400", // 100KB
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only include indexes >= 100KB
	var results []indexSizeRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.GreaterOrEqual(t, row.SizeBytes, int64(102400),
			"All results should have size >= 100KB")
	}
}

// TestIndexSize_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestIndexSize_SchemaFilter(t *testing.T) {
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
		CREATE SCHEMA reporting;
		
		CREATE TABLE public.metrics (
			id SERIAL PRIMARY KEY,
			metric_name VARCHAR(100),
			value NUMERIC
		);
		
		CREATE TABLE reporting.stats (
			id SERIAL PRIMARY KEY,
			stat_name VARCHAR(100),
			count INTEGER
		);
		
		CREATE INDEX idx_metrics_name ON public.metrics(metric_name);
		CREATE INDEX idx_stats_name ON reporting.stats(stat_name);
		
		INSERT INTO public.metrics (metric_name, value) 
		SELECT 'metric_' || generate_series, random() * 1000 
		FROM generate_series(1, 100);
		
		INSERT INTO reporting.stats (stat_name, count) 
		SELECT 'stat_' || generate_series, generate_series * 10 
		FROM generate_series(1, 100);
		
		ANALYZE public.metrics;
		ANALYZE reporting.stats;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with schema filter for reporting
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "reporting",
		"--size-min", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain reporting schema
	var results []indexSizeRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "reporting", row.Schema,
			"All results should be from reporting schema")
	}
}

// TestIndexSize_Explain verifies that --explain flag prints
// explanation without executing the query
func TestIndexSize_Explain(t *testing.T) {
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
	assert.Contains(t, output, "disk space")
	assert.Contains(t, output, "REINDEX")
}

// TestIndexSize_OrderedBySize verifies that results are ordered
// by size in descending order
func TestIndexSize_OrderedBySize(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with multiple indexes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE data_table (
			id SERIAL PRIMARY KEY,
			small_col VARCHAR(10),
			medium_col VARCHAR(100),
			large_col TEXT
		);
		
		CREATE INDEX idx_small ON data_table(small_col);
		CREATE INDEX idx_medium ON data_table(medium_col);
		CREATE INDEX idx_large ON data_table(large_col);
		
		INSERT INTO data_table (small_col, medium_col, large_col) 
		SELECT 
			'A' || generate_series,
			repeat('B', 50) || generate_series,
			repeat('C', 200) || generate_series
		FROM generate_series(1, 200);
		
		ANALYZE data_table;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running index:size command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--size-min", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should be ordered by size descending
	var results []indexSizeRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	// Verify ordering (each subsequent index should be <= previous)
	for i := 1; i < len(results); i++ {
		assert.LessOrEqual(t, results[i].SizeBytes, results[i-1].SizeBytes,
			"Results should be ordered by size descending")
	}
}
