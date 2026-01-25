package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDbManager_Connect_DirectURI(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A running PostgreSQL container and a DbManager instance
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := SetupTestPostgres(ctx, t)
	require.NoError(t, err, "Failed to setup test database")
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	manager := NewDbManager()

	// When: Connecting to the database using a direct connection URI
	conn, err := manager.Connect(ctx, testDB.ConnectionString())
	require.NoError(t, err, "Failed to connect to database")
	defer func() {
		assert.NoError(t, conn.Close(ctx))
	}()

	// Then: The connection should be established and executable queries should work
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "Failed to execute query")
	assert.Equal(t, 1, result, "Query should return 1")
}

func TestDbManager_Connect_PostgresScheme(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A running PostgreSQL container with postgres:// scheme URI
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := SetupTestPostgres(ctx, t)
	require.NoError(t, err, "Failed to setup test database")
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	manager := NewDbManager()

	// When: Connecting using postgres:// scheme
	conn, err := manager.Connect(ctx, testDB.ConnectionString())
	require.NoError(t, err, "Failed to connect with postgres:// scheme")
	defer func() {
		assert.NoError(t, conn.Close(ctx))
	}()

	// Then: The connection should work and return PostgreSQL version
	var version string
	err = conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	require.NoError(t, err, "Failed to query version")
	assert.Contains(t, version, "PostgreSQL", "Should be PostgreSQL")
}

func TestDbManager_Connect_InvalidURI(t *testing.T) {
	// Given: A DbManager instance and an invalid connection URI
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager := NewDbManager()
	invalidURI := "postgres://invalid:invalid@localhost:9999/invalid"

	// When: Attempting to connect with the invalid URI
	conn, err := manager.Connect(ctx, invalidURI)

	// Then: The connection should fail with an error and return nil connection
	assert.Error(t, err, "Should fail with invalid connection")
	assert.Nil(t, conn, "Connection should be nil on error")
}

func TestEncodePasswordInUri(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple password",
			input:    "postgres://user:pass@localhost:5432/db",
			expected: "postgres://user:pass@localhost:5432/db",
		},
		{
			name:     "Password with special characters",
			input:    "postgres://user:p@ss!w0rd@localhost:5432/db",
			expected: "postgres://user:p%40ss%21w0rd@localhost:5432/db",
		},
		{
			name:     "Password with quotes",
			input:    `postgres://user:p"a"ss@localhost:5432/db`,
			expected: "postgres://user:p%22a%22ss@localhost:5432/db",
		},
		{
			name:     "Password with colon",
			input:    "postgres://user:pass:word@localhost:5432/db",
			expected: "postgres://user:pass%3Aword@localhost:5432/db",
		},
		{
			name:     "No password",
			input:    "postgres://user@localhost:5432/db",
			expected: "postgres://user@localhost:5432/db",
		},
		{
			name:     "No credentials",
			input:    "postgres://localhost:5432/db",
			expected: "postgres://localhost:5432/db",
		},
		{
			name:     "PostgreSQL scheme",
			input:    "postgresql://user:p@ss@localhost:5432/db",
			expected: "postgresql://user:p%40ss@localhost:5432/db",
		},
		{
			name:     "Not a URI",
			input:    "some-config-name",
			expected: "some-config-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given: A connection string (from test case)
			// When: Encoding the password in the URI
			result := encodePasswordInUri(tt.input)

			// Then: The password should be properly URL-encoded
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDbManager_Connect_WithEncodedPassword(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Given: A PostgreSQL container with a properly encoded connection string
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := SetupTestPostgres(ctx, t)
	require.NoError(t, err, "Failed to setup test database")
	defer func() {
		assert.NoError(t, testDB.Close(ctx))
	}()

	manager := NewDbManager()

	// When: Connecting using the encoded connection string
	conn, err := manager.Connect(ctx, testDB.ConnectionString())
	require.NoError(t, err, "Failed to connect with encoded password")
	defer func() {
		assert.NoError(t, conn.Close(ctx))
	}()

	// Then: The connection should work and queries should execute successfully
	var result bool
	err = conn.QueryRow(ctx, "SELECT true").Scan(&result)
	require.NoError(t, err, "Failed to execute query")
	assert.True(t, result, "Query should return true")
}
