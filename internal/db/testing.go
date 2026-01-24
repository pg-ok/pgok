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
type TestPostgresContainer struct {
	container *postgres.PostgresContainer
	connStr   string
}

// SetupTestPostgres creates and starts a PostgreSQL container for testing
func SetupTestPostgres(ctx context.Context, t *testing.T) (*TestPostgresContainer, error) {
	t.Helper()

	// Create PostgreSQL container
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

	// Get connection string
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
func (tc *TestPostgresContainer) ConnectionString() string {
	return tc.connStr
}

// Close terminates the container
func (tc *TestPostgresContainer) Close(ctx context.Context) error {
	if tc.container != nil {
		return tc.container.Terminate(ctx)
	}
	return nil
}

// CreateConnection creates a new connection to the test database
func (tc *TestPostgresContainer) CreateConnection(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, tc.connStr)
}

// ExecSql executes SQL statements on the test database
func (tc *TestPostgresContainer) ExecSql(ctx context.Context, sql string) error {
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
