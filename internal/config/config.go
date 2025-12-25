package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

type DbConfig struct {
	Databases map[string]DatabaseConfig `toml:"db"`
}

type DatabaseConfig struct {
	URI string `toml:"uri"`
}

// Load attempts to load the configuration.
// If the file is missing, it returns an empty config instead of exiting.
// This allows the tool to work with direct DSNs even without a config file.
func Load() *DbConfig {
	configPath := "config/pgok.toml"

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Return empty config if file is missing.
		// We don't error out here because the user might provide a direct DSN.
		return &DbConfig{
			Databases: make(map[string]DatabaseConfig),
		}
	}

	var cfg DbConfig
	if _, err := toml.DecodeFile(configPath, &cfg); err != nil {
		// However, if the file exists but is invalid, we should warn the user.
		fmt.Fprintf(os.Stderr, "Warning: Failed to parse config/pgok.toml: %v\n", err)
		// Return empty to avoid panic, assuming user might fix it or use DSN.
		return &DbConfig{
			Databases: make(map[string]DatabaseConfig),
		}
	}

	return &cfg
}

func (c *DbConfig) GetDbURI(name string) string {
	if db, ok := c.Databases[name]; ok {
		return db.URI
	}

	// If the user requested an alias, but we couldn't find it
	fmt.Fprintf(os.Stderr, "Error: Database alias '%s' not found.\n", name)

	if len(c.Databases) == 0 {
		fmt.Fprintln(os.Stderr, "Tip: No config file loaded (or it is empty).")
		fmt.Fprintln(os.Stderr, "To use aliases, create 'config/pgok.toml'.")
		fmt.Fprintln(os.Stderr, "Otherwise, provide a full connection string: postgres://user:pass@host/db")
	} else {
		fmt.Fprintln(os.Stderr, "Available aliases:", c.GetDatabaseNames())
	}

	os.Exit(1)
	return ""
}

func (c *DbConfig) GetDatabaseNames() []string {
	keys := make([]string, 0, len(c.Databases))
	for k := range c.Databases {
		keys = append(keys, k)
	}
	return keys
}
