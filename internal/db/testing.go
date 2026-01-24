package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestPostgresContainer manages a PostgreSQL test container
// Used in integration tests to provide an isolated database instance
type TestPostgresContainer struct {
	container *postgres.PostgresContainer
	connStr   string
}

// SetupTestPostgres creates and starts a PostgreSQL container for testing
//
// Given: A test context and testing.T instance
// When: Called at the beginning of an integration test
// Then: Returns a running PostgreSQL container ready for testing
func SetupTestPostgres(ctx context.Context, t *testing.T) (*TestPostgresContainer, error) {
	t.Helper()

	// Start PostgreSQL container with test configuration
	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	// Retrieve the connection string for the running container
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection string: %w", err)
	}

	return &TestPostgresContainer{
		container: container,
		connStr:   connStr,
	}, nil
}

// ConnectionString returns the connection string for the test database
//
// Given: A running test container
// When: Called to get the connection URI
// Then: Returns a valid postgres:// connection string
func (tc *TestPostgresContainer) ConnectionString() string {
	return tc.connStr
}

// Close terminates the container and cleans up resources
//
// Given: A running test container
// When: Called at the end of a test (typically in defer)
// Then: The container is stopped and removed
func (tc *TestPostgresContainer) Close(ctx context.Context) error {
	if tc.container != nil {
		return tc.container.Terminate(ctx)
	}
	return nil
}

// CreateConnection creates a new connection to the test database
//
// Given: A running test container
// When: A new database connection is needed
// Then: Returns an established pgx connection
func (tc *TestPostgresContainer) CreateConnection(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, tc.connStr)
}

// ExecSQL executes SQL statements on the test database
//
// Given: A running test container and SQL statement(s)
// When: Need to setup test data or modify database state
// Then: The SQL is executed and the connection is automatically closed
func (tc *TestPostgresContainer) ExecSQL(ctx context.Context, sql string) error {
	conn, err := tc.CreateConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to execute SQL: %w", err)
	}

	return nil
}
