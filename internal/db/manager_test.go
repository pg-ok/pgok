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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup test PostgreSQL container
	testDB, err := SetupTestPostgres(ctx, t)
	require.NoError(t, err, "Failed to setup test database")
	defer testDB.Close(ctx)

	// Create manager
	manager := NewDbManager()

	// Test connection with direct URI
	conn, err := manager.Connect(ctx, testDB.ConnectionString())
	require.NoError(t, err, "Failed to connect to database")
	defer conn.Close(ctx)

	// Verify connection works
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "Failed to execute query")
	assert.Equal(t, 1, result, "Query should return 1")
}

func TestDbManager_Connect_PostgresScheme(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	testDB, err := SetupTestPostgres(ctx, t)
	require.NoError(t, err, "Failed to setup test database")
	defer testDB.Close(ctx)

	manager := NewDbManager()

	// Test with postgres:// scheme
	conn, err := manager.Connect(ctx, testDB.ConnectionString())
	require.NoError(t, err, "Failed to connect with postgres:// scheme")
	defer conn.Close(ctx)

	var version string
	err = conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	require.NoError(t, err, "Failed to query version")
	assert.Contains(t, version, "PostgreSQL", "Should be PostgreSQL")
}

func TestDbManager_Connect_InvalidURI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager := NewDbManager()

	// Test with invalid URI
	conn, err := manager.Connect(ctx, "postgres://invalid:invalid@localhost:9999/invalid")
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
			result := encodePasswordInUri(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDbManager_Connect_WithEncodedPassword(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup test PostgreSQL container with special characters in password
	testDB, err := SetupTestPostgres(ctx, t)
	require.NoError(t, err, "Failed to setup test database")
	defer testDB.Close(ctx)

	manager := NewDbManager()

	// The connection string from testcontainers should already be properly encoded
	conn, err := manager.Connect(ctx, testDB.ConnectionString())
	require.NoError(t, err, "Failed to connect with encoded password")
	defer conn.Close(ctx)

	// Verify connection
	var result bool
	err = conn.QueryRow(ctx, "SELECT true").Scan(&result)
	require.NoError(t, err, "Failed to execute query")
	assert.True(t, result, "Query should return true")
}
