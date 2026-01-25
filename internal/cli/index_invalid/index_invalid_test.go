package index_invalid

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

// TestIndexInvalid_WithValidIndexes verifies that index:invalid correctly
// reports when all indexes are valid
func TestIndexInvalid_WithValidIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with only valid indexes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with valid indexes
	setupSQL := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			username VARCHAR(100) NOT NULL
		);
		
		CREATE UNIQUE INDEX idx_users_email ON users(email);
		CREATE INDEX idx_users_username ON users(username);
		
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

	// When: Running the index:invalid command
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

	// Then: The output should indicate no broken indexes
	assert.Contains(t, output, "Validating indexes")
	assert.Contains(t, output, "No broken indexes found")
}

// TestIndexInvalid_JSONOutputValid verifies that index:invalid produces
// valid JSON output (empty array when all indexes are valid)
func TestIndexInvalid_JSONOutputValid(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with valid indexes
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
		
		CREATE INDEX idx_products_name ON products(name);
		
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

	// Then: The output should be valid JSON (empty array)
	var results []invalidRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")
	assert.Equal(t, 0, len(results), "Should have no invalid indexes")
}

// TestIndexInvalid_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestIndexInvalid_SchemaFilter(t *testing.T) {
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
		
		CREATE INDEX idx_public_data ON public.public_table(data);
		CREATE INDEX idx_schema_data ON test_schema.schema_table(data);
		
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
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: Output should reference the test_schema
	assert.Contains(t, output, "test_schema")
}

// TestIndexInvalid_Explain verifies that --explain flag prints
// explanation without executing the query
func TestIndexInvalid_Explain(t *testing.T) {
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
	assert.Contains(t, output, "invalid")
	assert.Contains(t, output, "CREATE INDEX CONCURRENTLY")
}

// TestIndexInvalid_AllSchemas verifies that index:invalid can scan
// all schemas with wildcard filter
func TestIndexInvalid_AllSchemas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with multiple schemas
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE SCHEMA schema_a;
		CREATE SCHEMA schema_b;
		
		CREATE TABLE schema_a.table_a (
			id SERIAL PRIMARY KEY,
			value INTEGER
		);
		
		CREATE TABLE schema_b.table_b (
			id SERIAL PRIMARY KEY,
			value INTEGER
		);
		
		CREATE INDEX idx_a_value ON schema_a.table_a(value);
		CREATE INDEX idx_b_value ON schema_b.table_b(value);
		
		INSERT INTO schema_a.table_a (value) 
		SELECT generate_series FROM generate_series(1, 20);
		
		INSERT INTO schema_b.table_b (value) 
		SELECT generate_series FROM generate_series(1, 20);
		
		ANALYZE schema_a.table_a;
		ANALYZE schema_b.table_b;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with wildcard schema filter
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "*",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: Output should indicate all schemas were scanned
	assert.Contains(t, output, "ALL (except system)")
}
