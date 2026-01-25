package sequence_overflow

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

// TestSequenceOverflow_WithSequences verifies that sequence:overflow correctly
// reports sequence usage percentages
func TestSequenceOverflow_WithSequences(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with sequences
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	// Create test schema with sequences
	setupSQL := `
		CREATE TABLE orders (
			id SERIAL PRIMARY KEY,
			order_number VARCHAR(50)
		);
		
		CREATE SEQUENCE custom_seq START 1;
		
		INSERT INTO orders (order_number) 
		SELECT 'ORD-' || generate_series 
		FROM generate_series(1, 100);
		
		-- Advance custom sequence
		SELECT nextval('custom_seq') FROM generate_series(1, 50);
		
		ANALYZE orders;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout for table output
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the sequence:overflow command
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--used-percent-min", "0",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should contain sequence usage information
	assert.Contains(t, output, "Checking sequence usage")
	assert.Contains(t, output, "USED")
}

// TestSequenceOverflow_JSONOutput verifies that sequence:overflow produces
// valid JSON output with correct structure
func TestSequenceOverflow_JSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with sequences
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE TABLE invoices (
			id SERIAL PRIMARY KEY,
			invoice_number VARCHAR(50)
		);
		
		INSERT INTO invoices (invoice_number) 
		SELECT 'INV-' || generate_series 
		FROM generate_series(1, 75);
		
		ANALYZE invoices;
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
		"--used-percent-min", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: The output should be valid JSON
	var results []sequenceUsageRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err, "Output should be valid JSON")

	// Verify JSON structure if results exist
	if len(results) > 0 {
		assert.NotEmpty(t, results[0].Schema)
		assert.NotEmpty(t, results[0].Sequence)
		assert.NotEmpty(t, results[0].DataType)
		assert.GreaterOrEqual(t, results[0].UsedPercent, 0.0)
		assert.GreaterOrEqual(t, results[0].LastValue, int64(0))
		assert.Greater(t, results[0].MaxValue, int64(0))
	}
}

// TestSequenceOverflow_UsedPercentFilter verifies that --used-percent-min filter
// correctly excludes sequences below the threshold
func TestSequenceOverflow_UsedPercentFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with sequences at different usage levels
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE SEQUENCE low_usage_seq START 1 MAXVALUE 1000000;
		CREATE SEQUENCE medium_usage_seq START 1 MAXVALUE 1000;
		
		-- Advance low usage sequence slightly
		SELECT nextval('low_usage_seq') FROM generate_series(1, 10);
		
		-- Advance medium usage sequence more
		SELECT nextval('medium_usage_seq') FROM generate_series(1, 100);
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with used-percent-min filter set to 5%
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--used-percent-min", "5.0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only include sequences with >= 5% usage
	var results []sequenceUsageRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.GreaterOrEqual(t, row.UsedPercent, 5.0,
			"All results should have used percent >= 5.0")
	}
}

// TestSequenceOverflow_SchemaFilter verifies that the --schema filter
// correctly limits results to the specified schema
func TestSequenceOverflow_SchemaFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with sequences in different schemas
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		CREATE SCHEMA operations;
		
		CREATE TABLE public.public_records (
			id SERIAL PRIMARY KEY,
			data TEXT
		);
		
		CREATE TABLE operations.operation_logs (
			id SERIAL PRIMARY KEY,
			message TEXT
		);
		
		INSERT INTO public.public_records (data) 
		SELECT 'Record ' || generate_series 
		FROM generate_series(1, 30);
		
		INSERT INTO operations.operation_logs (message) 
		SELECT 'Log ' || generate_series 
		FROM generate_series(1, 30);
		
		ANALYZE public.public_records;
		ANALYZE operations.operation_logs;
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with schema filter for operations
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "operations",
		"--used-percent-min", "0",
		"--output", "json",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)

	// Then: Results should only contain operations schema
	var results []sequenceUsageRow
	err = json.Unmarshal(capturedOutput, &results)
	require.NoError(t, err)

	for _, row := range results {
		assert.Equal(t, "operations", row.Schema,
			"All results should be from operations schema")
	}
}

// TestSequenceOverflow_Explain verifies that --explain flag prints
// explanation without executing the query
func TestSequenceOverflow_Explain(t *testing.T) {
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
	assert.Contains(t, output, "maximum limits")
	assert.Contains(t, output, "BIGINT")
}

// TestSequenceOverflow_HighUsageWarning verifies that sequences with
// high usage percentage are properly flagged
func TestSequenceOverflow_HighUsageWarning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A test database with a sequence at high usage
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := db.SetupTestPostgres(ctx, t)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	setupSQL := `
		-- Create sequence with low max value to simulate high usage
		CREATE SEQUENCE high_usage_seq START 1 MAXVALUE 100;
		
		-- Advance sequence to 85 (85% usage)
		SELECT nextval('high_usage_seq') FROM generate_series(1, 85);
	`
	err = testDB.ExecSQL(ctx, setupSQL)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running with low threshold to catch high usage
	cmd := NewCommand()
	cmd.SetArgs([]string{
		testDB.ConnectionString(),
		"--schema", "public",
		"--used-percent-min", "80.0",
		"--output", "table",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: Output should show warning indicator for high usage
	assert.Contains(t, output, "[!]")
}
