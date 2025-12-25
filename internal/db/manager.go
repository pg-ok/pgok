package db

import (
	"context"
	"net/url"
	"strings"

	"github.com/pg-ok/pgok/internal/config"

	"github.com/jackc/pgx/v5"
)

type DbManager struct {
	config *config.DbConfig
}

func NewDbManager() *DbManager {
	return &DbManager{
		config: config.Load(),
	}
}

// Connect establishes a connection to the database.
// If `dbUriOrConfigName` starts with "postgres://" or "postgresql://" -> treat as a direct connection URI.
// Otherwise -> treat as an alias and look it up in the config.
func (m *DbManager) Connect(ctx context.Context, dbUriOrConfigName string) (*pgx.Conn, error) {
	var rawURI string

	if strings.HasPrefix(dbUriOrConfigName, "postgres://") || strings.HasPrefix(dbUriOrConfigName, "postgresql://") {
		rawURI = dbUriOrConfigName
	} else {
		rawURI = m.config.GetDbURI(dbUriOrConfigName)
	}

	safeUri := encodePasswordInUri(rawURI)

	conn, err := pgx.Connect(ctx, safeUri)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (m *DbManager) GetConfigDatabaseNames() []string {
	return m.config.GetDatabaseNames()
}

// encodePasswordInUri parses the connection string and URL-encodes the password.
// Logic:
// 1. Strip the scheme "postgres://".
// 2. Find the host/credentials separator — the last "@" ("at" sign), since we read from the right.
// 3. Split the login and password by the first colon ":".
// 4. Encode the password and reassemble the string.
func encodePasswordInUri(rawURI string) string {
	// Determine the scheme
	var scheme string
	if strings.HasPrefix(rawURI, "postgres://") {
		scheme = "postgres://"
	} else if strings.HasPrefix(rawURI, "postgresql://") {
		scheme = "postgresql://"
	} else {
		// Doesn't look like a URI, return as is (e.g., DSN)
		return rawURI
	}

	// Remove the scheme from the beginning
	rest := rawURI[len(scheme):]

	// Find the last "@" ("at" sign), since we scan "from the right" (from host to user)
	lastAt := strings.LastIndex(rest, "@")
	if lastAt == -1 {
		// No "@" symbol — no password
		return rawURI
	}

	// credentialsPart: "user:pass"
	credentialsPart := rest[:lastAt]
	// hostPart: "host:5432/db..."
	hostPart := rest[lastAt+1:]

	// Find the first colon separating user and password
	firstColon := strings.Index(credentialsPart, ":")
	if firstColon == -1 {
		// No colon — means only user is specified, no password
		return rawURI
	}

	user := credentialsPart[:firstColon]
	password := credentialsPart[firstColon+1:]

	// Encode the password (transforms M""4 into M%22%224)
	encodedPassword := url.QueryEscape(password)

	// Reassemble everything
	return scheme + user + ":" + encodedPassword + "@" + hostPart
}
