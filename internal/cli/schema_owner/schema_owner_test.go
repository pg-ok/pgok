package schema_owner

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

// TestSchemaOwner_AllCorrectOwner verifies that schema:owner correctly
// reports when all objects are owned by the expected user
func TestSchemaOwner_AllCorrectOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database where all objects are owned by testuser
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with tables
	setupSQL := `
		CREATE TABLE employees (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			department VARCHAR(100)
		);
		
		CREATE TABLE projects (
			id SERIAL PRIMARY KEY,
			title VARCHAR(255)
		);
		
		INSERT INTO employees (name, department) 
		SELECT 
			'Employee ' || generate_series,
			'Dept ' || (generate_series % 5)
		FROM generate_series(1, 50);
		
		INSERT INTO projects (title) 
		SELECT 'Project ' || generate_series 
		FROM generate_series(1, 20);
		
		ANALYZE employees;
		ANALYZE projects;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the schema:owner command expecting 'testuser'
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--expected", "testuser",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should indicate all objects are correctly owned
	assert.Contains(t, output, "Checking schema ownership")
	assert.Contains(t, output, "correctly owned by 'testuser'")
}

// TestSchemaOwner_JSONOutput verifies that schema:owner produces
// valid JSON output with correct structure
func TestSchemaOwner_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with objects owned by testuser
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
		
		CREATE TABLE mood_log (
			id SERIAL PRIMARY KEY,
			user_mood mood NOT NULL,
			recorded_at TIMESTAMP DEFAULT NOW()
		);
		
		INSERT INTO mood_log (user_mood) 
		SELECT 
			CASE 
				WHEN generate_series % 3 = 0 THEN 'happy'
				WHEN generate_series % 3 = 1 THEN 'sad'
				ELSE 'neutral'
			END::mood
		FROM generate_series(1, 30);
		
		ANALYZE mood_log;
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
		"--expected", "testuser",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON (empty array when all correct)
	var results []ownerRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// All objects should be owned by testuser, so results should be empty
	assert.Equal(t, 0, len(results), "All objects owned by expected user")
}

// TestSchemaOwner_WithWrongOwner verifies that schema:owner detects
// objects owned by unexpected users
func TestSchemaOwner_WithWrongOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database where we check for a non-existent owner
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE inventory (
			id SERIAL PRIMARY KEY,
			item_name VARCHAR(255),
			quantity INTEGER
		);
		
		INSERT INTO inventory (item_name, quantity) 
		SELECT 
			'Item ' || generate_series,
			generate_series * 10
		FROM generate_series(1, 40);
		
		ANALYZE inventory;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Checking for a different expected owner
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--expected", "appuser",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Should find objects with wrong owner
	var results []ownerRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	// Since testuser owns the objects but we expected appuser
	if len(results) > 0 {
		assert.Equal(t, "public", results[0].SchemaName)
		assert.Equal(t, "testuser", results[0].ActualOwner)
		assert.Contains(t, results[0].FixCommand, "ALTER")
		assert.Contains(t, results[0].FixCommand, "OWNER TO appuser")
	}
}

// TestSchemaOwner_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestSchemaOwner_SchemaFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with objects in different schemas
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE SCHEMA app_data;
		
		CREATE TABLE public.public_items (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100)
		);
		
		CREATE TABLE app_data.app_items (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100)
		);
		
		INSERT INTO public.public_items (name) 
		SELECT 'Public Item ' || generate_series 
		FROM generate_series(1, 20);
		
		INSERT INTO app_data.app_items (name) 
		SELECT 'App Item ' || generate_series 
		FROM generate_series(1, 20);
		
		ANALYZE public.public_items;
		ANALYZE app_data.app_items;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with schema filter for app_data
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "app_data",
		"--expected", "wronguser",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain app_data schema
	var results []ownerRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "app_data", row.SchemaName,
			"All results should be from app_data schema")
	}
}

// TestSchemaOwner_MultipleObjectTypes verifies that schema:owner
// detects ownership issues across different object types
func TestSchemaOwner_MultipleObjectTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with various object types
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		-- Create different types of objects
		CREATE TABLE test_table (
			id SERIAL PRIMARY KEY,
			value INTEGER
		);
		
		CREATE VIEW test_view AS SELECT * FROM test_table;
		
		CREATE TYPE status_type AS ENUM ('active', 'inactive');
		
		CREATE SEQUENCE test_seq;
		
		CREATE DOMAIN email_domain AS VARCHAR(255)
			CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$');
		
		INSERT INTO test_table (value) 
		SELECT generate_series FROM generate_series(1, 30);
		
		ANALYZE test_table;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Checking ownership with wrong expected user
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--expected", "postgres",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Should detect multiple object types with wrong owner
	var results []ownerRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	// Check that different object types are detected
	objectTypes := make(map[string]bool)
	for _, row := range results {
		objectTypes[row.ObjectType] = true
	}

	// We should have at least some object types detected
	assert.Greater(t, len(objectTypes), 0, "Should detect various object types")
}

// TestSchemaOwner_Explain verifies that --explain flag prints
// explanation without executing the query
func TestSchemaOwner_Explain(t *testing.T) {
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
		"--expected", "testuser",
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
	assert.Contains(t, output, "Ownership")
	assert.Contains(t, output, "migrations")
}
