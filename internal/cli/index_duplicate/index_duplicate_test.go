package index_duplicate

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

// TestIndexDuplicate_WithDuplicates verifies that index:duplicate correctly
// detects duplicate indexes with the same definition
func TestIndexDuplicate_WithDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with duplicate indexes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with duplicate indexes
	setupSQL := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			username VARCHAR(100) NOT NULL
		);
		
		-- Create first index
		CREATE INDEX idx_users_email_1 ON users(email);
		
		-- Create duplicate index with exact same definition
		CREATE INDEX idx_users_email_2 ON users(email);
		
		-- Create another duplicate
		CREATE INDEX idx_users_email_3 ON users(email);
		
		-- Insert some data
		INSERT INTO users (email, username) 
		SELECT 
			'user' || generate_series || '@example.com',
			'user' || generate_series
		FROM generate_series(1, 100);
		
		ANALYZE users;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the index:duplicate command
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

	// Then: The output should contain duplicate index information
	assert.Contains(t, output, "DUPLICATE indexes")
	assert.Contains(t, output, "public")
	assert.Contains(t, output, "KEEP INDEX")
	assert.Contains(t, output, "DROP DUPLICATE")
}

// TestIndexDuplicate_JSONOutput verifies that index:duplicate produces
// valid JSON output with correct structure
func TestIndexDuplicate_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with duplicate indexes
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
		
		CREATE INDEX idx_products_name_1 ON products(name);
		CREATE INDEX idx_products_name_2 ON products(name);
		
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
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON
	var results []duplicateRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// Verify JSON structure
	require.Greater(t, len(results), 0, "Should find duplicate indexes")
	assert.Equal(t, "public", results[0].Schema)
	assert.NotEmpty(t, results[0].KeepIndex)
	assert.NotEmpty(t, results[0].DropIndexes)
	assert.Greater(t, len(results[0].DropIndexes), 0)
	assert.Greater(t, results[0].SizeBytes, int64(0))
}

// TestIndexDuplicate_NoDuplicates verifies that index:duplicate handles
// the case when no duplicate indexes exist
func TestIndexDuplicate_NoDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with unique indexes only
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE orders (
			id SERIAL PRIMARY KEY,
			customer_id INTEGER NOT NULL,
			status VARCHAR(50) NOT NULL
		);
		
		-- Create unique indexes (no duplicates)
		CREATE INDEX idx_orders_customer ON orders(customer_id);
		CREATE INDEX idx_orders_status ON orders(status);
		
		INSERT INTO orders (customer_id, status) 
		SELECT 
			generate_series % 20,
			CASE WHEN generate_series % 2 = 0 THEN 'pending' ELSE 'completed' END
		FROM generate_series(1, 100);
		
		ANALYZE orders;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the index:duplicate command
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

	// Then: Output should indicate no duplicates found
	assert.Contains(t, output, "No duplicate indexes found")
}

// TestIndexDuplicate_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestIndexDuplicate_SchemaFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with duplicates in different schemas
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
		
		-- Duplicates in public schema
		CREATE INDEX idx_public_data_1 ON public.public_table(data);
		CREATE INDEX idx_public_data_2 ON public.public_table(data);
		
		-- Duplicates in test_schema
		CREATE INDEX idx_schema_data_1 ON test_schema.schema_table(data);
		CREATE INDEX idx_schema_data_2 ON test_schema.schema_table(data);
		
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
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain test_schema duplicates
	var results []duplicateRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "test_schema", row.Schema,
			"All results should be from test_schema")
	}
}

// TestIndexDuplicate_Explain verifies that --explain flag prints
// explanation without executing the query
func TestIndexDuplicate_Explain(t *testing.T) {
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
	assert.Contains(t, output, "duplicate")
}
