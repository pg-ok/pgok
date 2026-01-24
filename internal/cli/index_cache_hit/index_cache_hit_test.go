package index_cache_hit

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

// TestIndexCacheHit_WithIndexes verifies that index:cache-hit correctly
// detects indexes and reports their cache hit ratios
func TestIndexCacheHit_WithIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with a table and indexes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with table and indexes
	setupSQL := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			username VARCHAR(100) NOT NULL
		);
		CREATE UNIQUE INDEX idx_users_email ON users(email);
		CREATE INDEX idx_users_username ON users(username);
		
		-- Insert some data to generate statistics
		INSERT INTO users (email, username) 
		SELECT 
			'user' || generate_series || '@example.com',
			'user' || generate_series
		FROM generate_series(1, 100);
		
		-- Force statistics collection
		ANALYZE users;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the index:cache-hit command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--calls-min", "0", // Set to 0 to catch all indexes
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should contain index information
	assert.Contains(t, output, "Index Cache Hit Ratio")
	assert.Contains(t, output, "public.users")
}

// TestIndexCacheHit_JSONOutput verifies that index:cache-hit produces
// valid JSON output with correct structure
func TestIndexCacheHit_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with indexed table
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
			name VARCHAR(255) NOT NULL
		);
		
		INSERT INTO products (name) 
		SELECT 'Product ' || generate_series 
		FROM generate_series(1, 50);
		
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
		"--calls-min", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON
	var results []cacheHitRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// Verify JSON structure
	if len(results) > 0 {
		assert.NotEmpty(t, results[0].Schema)
		assert.NotEmpty(t, results[0].Table)
		assert.NotEmpty(t, results[0].Index)
		assert.GreaterOrEqual(t, results[0].HitRatio, 0.0)
		assert.GreaterOrEqual(t, results[0].DiskReads, int64(0))
		assert.GreaterOrEqual(t, results[0].MemoryHits, int64(0))
	}
}

// TestIndexCacheHit_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestIndexCacheHit_SchemaFilter(t *testing.T) {
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
		CREATE SCHEMA test_schema;
		
		CREATE TABLE public.public_table (
			id SERIAL PRIMARY KEY,
			data VARCHAR(100)
		);
		
		CREATE TABLE test_schema.schema_table (
			id SERIAL PRIMARY KEY,
			data VARCHAR(100)
		);
		
		INSERT INTO public.public_table (data) 
		SELECT 'data' || generate_series FROM generate_series(1, 10);
		
		INSERT INTO test_schema.schema_table (data) 
		SELECT 'data' || generate_series FROM generate_series(1, 10);
		
		ANALYZE public.public_table;
		ANALYZE test_schema.schema_table;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with schema filter for test_schema
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "test_schema",
		"--calls-min", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain test_schema tables
	var results []cacheHitRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "test_schema", row.Schema, 
			"All results should be from test_schema")
	}
}

// TestIndexCacheHit_Explain verifies that --explain flag prints
// explanation without executing the query
func TestIndexCacheHit_Explain(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: Valid database connection string (doesn't need to be accessible)
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
	assert.Contains(t, output, "shared_buffers")
}
