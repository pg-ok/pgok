package cmd

import (
	"os"

	"github.com/pg-ok/pgok/internal/cli/app_db_list"
	"github.com/pg-ok/pgok/internal/cli/index_cache_hit"
	"github.com/pg-ok/pgok/internal/cli/index_duplicate"
	"github.com/pg-ok/pgok/internal/cli/index_invalid"
	"github.com/pg-ok/pgok/internal/cli/index_missing"
	"github.com/pg-ok/pgok/internal/cli/index_missing_fk"
	"github.com/pg-ok/pgok/internal/cli/index_size"
	"github.com/pg-ok/pgok/internal/cli/index_unused"
	"github.com/pg-ok/pgok/internal/cli/schema_owner"
	"github.com/pg-ok/pgok/internal/cli/sequence_overflow"
	"github.com/pg-ok/pgok/internal/cli/table_missing_pk"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pgok",
	Short: "A CLI tool to analyze PG databases",
	Long:  "pgok is a CLI utility for analyzing PostgreSQL database health, state, and performance.",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddGroup(&cobra.Group{ID: "app", Title: "App Commands"})
	rootCmd.AddGroup(&cobra.Group{ID: "index", Title: "Index Commands"})
	rootCmd.AddGroup(&cobra.Group{ID: "schema", Title: "Schema Commands"})
	rootCmd.AddGroup(&cobra.Group{ID: "sequence", Title: "Sequence Commands"})
	rootCmd.AddGroup(&cobra.Group{ID: "table", Title: "Table Commands"})

	rootCmd.AddCommand(app_db_list.NewCommand())
	rootCmd.AddCommand(index_cache_hit.NewCommand())
	rootCmd.AddCommand(index_duplicate.NewCommand())
	rootCmd.AddCommand(index_invalid.NewCommand())
	rootCmd.AddCommand(index_missing.NewCommand())
	rootCmd.AddCommand(index_missing_fk.NewCommand())
	rootCmd.AddCommand(index_size.NewCommand())
	rootCmd.AddCommand(index_unused.NewCommand())
	rootCmd.AddCommand(schema_owner.NewCommand())
	rootCmd.AddCommand(sequence_overflow.NewCommand())
	rootCmd.AddCommand(table_missing_pk.NewCommand())
}
