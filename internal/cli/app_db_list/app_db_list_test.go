package app_db_list

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppDbList_WithConfigFile verifies that app:db:list correctly displays
// database names from a valid config file

func TestAppDbList_WithConfigFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A temporary config file with two database entries
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "config")
	err := os.MkdirAll(configDir, 0755)
	require.NoError(t, err)

	configPath := filepath.Join(configDir, "pgok.toml")
	configContent := `[db]
db_test1 = { uri = "postgres://user:pass@localhost:5432/test1" }
db_test2 = { uri = "postgres://user:pass@localhost:5432/test2" }
`
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Change to temp directory for test
	origDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(origDir)
	}()

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	// Capture stdout
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = origStdout }()

	// When: Running the app:db:list command
	cmd := NewCommand()
	err = cmd.Execute()
	require.NoError(t, err)

	// Restore stdout and read captured output
	_ = w.Close()
	os.Stdout = origStdout
	capturedOutput, _ := io.ReadAll(r)
	output := string(capturedOutput)

	// Then: The output should contain both database names
	assert.Contains(t, output, "Configured databases:")
	assert.Contains(t, output, "db_test1")
	assert.Contains(t, output, "db_test2")
}

// TestAppDbList_WithoutConfigFile verifies that app:db:list handles
// missing config file gracefully by showing appropriate error message
func TestAppDbList_WithoutConfigFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A temporary directory without any config file
	tmpDir := t.TempDir()
	
	origDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(origDir)
	}()

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	// Capture stdout and stderr
	origStdout := os.Stdout
	origStderr := os.Stderr
	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = wOut
	os.Stderr = wErr
	defer func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()

	// When: Running the app:db:list command
	cmd := NewCommand()
	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read outputs
	_ = wOut.Close()
	_ = wErr.Close()
	os.Stdout = origStdout
	os.Stderr = origStderr
	
	capturedOut, _ := io.ReadAll(rOut)
	capturedErr, _ := io.ReadAll(rErr)
	output := string(capturedOut)
	errOutput := string(capturedErr)

	// Then: The output should indicate no databases found
	assert.Contains(t, output, "Configured databases:")
	assert.Contains(t, errOutput, "No databases found in config/pgok.toml")
}

// TestAppDbList_WithEmptyConfig verifies that app:db:list handles
// empty config file (no databases defined) with appropriate message
func TestAppDbList_WithEmptyConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A temporary config file with empty database section
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "config")
	err := os.MkdirAll(configDir, 0755)
	require.NoError(t, err)

	configPath := filepath.Join(configDir, "pgok.toml")
	configContent := `[db]
`
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	origDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(origDir)
	}()

	err = os.Chdir(tmpDir)
	require.NoError(t, err)

	// Capture stdout and stderr
	origStdout := os.Stdout
	origStderr := os.Stderr
	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = wOut
	os.Stderr = wErr
	defer func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()

	// When: Running the app:db:list command with empty config
	cmd := NewCommand()
	err = cmd.Execute()
	require.NoError(t, err)

	// Restore and read outputs
	_ = wOut.Close()
	_ = wErr.Close()
	os.Stdout = origStdout
	os.Stderr = origStderr
	
	capturedOut, _ := io.ReadAll(rOut)
	capturedErr, _ := io.ReadAll(rErr)
	output := string(capturedOut)
	errOutput := string(capturedErr)

	// Then: The output should indicate no databases found
	assert.Contains(t, output, "Configured databases:")
	assert.Contains(t, errOutput, "No databases found in config/pgok.toml")
}
