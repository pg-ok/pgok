package index_missing_fk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pg-ok/pgok/internal/db"
	"github.com/pg-ok/pgok/internal/util"

	"github.com/jackc/pgx/v5"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

type Options struct {
	DbName  string
	Schema  string
	Explain bool
	Output  util.OutputFormat
}

func NewCommand() *cobra.Command {
	opts := &Options{
		// Default to scanning all schemas
		Schema: "*",

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "index",

		Use: "index:missing-fk [db_name]",

		Short: "Find foreign keys that lack an index on the child table",

		Long: `Find foreign keys that lack an index on the child table.
Missing indexes on Foreign Keys can cause severe locking issues (locks on parent table propagate to child) 
and slow down DELETE/UPDATE operations on the parent table.`,

		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			opts.DbName = args[0]
			run(opts)
		},
	}

	flags := command.Flags()
	flags.StringVar(&opts.Schema, "schema", opts.Schema, "Schema name (use '*' for all user schemas)")
	flags.BoolVar(&opts.Explain, "explain", false, "Print the SQL query and explain the logic/interpretation")

	flags.Var(&opts.Output, "output", "Output format (table, json)")
	_ = command.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json"}, cobra.ShellCompDirectiveDefault
	})

	return command
}

type fkMissingRow struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	ForeignKey string `json:"foreign_key"`
	Definition string `json:"definition"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	/*
	 * This SQL query searches for Foreign Keys that lack an index
	 * where the FK columns match the index's leading columns.
	 */
	rawSql := `
       SELECT
          n.nspname AS schema_name,
          cl.relname AS table_name,
          c.conname AS foreign_key,
          pg_get_constraintdef(c.oid) AS definition
       FROM pg_constraint AS c
       JOIN pg_namespace AS n ON n.oid = c.connamespace
       JOIN pg_class AS cl ON cl.oid = c.conrelid
       WHERE c.contype = 'f' -- Only Foreign Keys
       AND ($1 = '*' OR n.nspname = $1)
       AND n.nspname NOT IN ('pg_catalog', 'information_schema')
       AND n.nspname NOT LIKE 'pg_toast%'
       AND NOT EXISTS (
          SELECT 1
          FROM pg_index AS i
          WHERE i.indrelid = c.conrelid
          AND i.indisvalid
          -- Check if the FK columns match the *prefix* of the index columns.
          -- conkey: array of FK columns
          -- indkey: array of index columns (cast to int2[] for comparison)
          -- Slicing [1: ...] takes a prefix of the index array with the same length as the FK.
          AND (i.indkey::int2[])[1:array_length(c.conkey, 1)] = c.conkey::int2[]
       )
       ORDER BY schema_name, table_name, foreign_key;
    `

	sqlQuery := util.TrimLeftSpaces(rawSql)

	if opts.Explain {
		printExplanation(sqlQuery, opts)
		return
	}

	ctx := context.Background()
	conn, err := manager.Connect(ctx, opts.DbName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	defer func(conn *pgx.Conn, ctx context.Context) {
		err := conn.Close(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error closing connection: %v\n", err)
		}
	}(conn, ctx)

	rows, err := conn.Query(ctx, sqlQuery, opts.Schema)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []fkMissingRow

	for rows.Next() {
		var r fkMissingRow

		err := rows.Scan(
			&r.Schema,
			&r.Table,
			&r.ForeignKey,
			&r.Definition,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Row scan failed: %v\n", err)
			os.Exit(1)
		}

		results = append(results, r)
	}

	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "Rows iteration failed: %v\n", rows.Err())
		os.Exit(1)
	}

	switch opts.Output {
	case util.OutputFormatJson:
		jsonData, _ := json.MarshalIndent(results, "", "  ")
		fmt.Println(string(jsonData))

	default:
		schemaDisplay := opts.Schema
		if opts.Schema == "*" {
			schemaDisplay = "ALL (except system)"
		}

		fmt.Printf("Searching for missing Foreign Key indexes in `%s`\n", opts.DbName)
		fmt.Printf("Schema: %s\n", schemaDisplay)

		if len(results) == 0 {
			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("No missing FK indexes found. Your data integrity performance is safe! 🔒")
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Schema", "Table", "Foreign Key", "Definition"})

		for _, row := range results {
			// Truncate definition for display purposes only (in Raw mode)
			definitionDisplay := row.Definition
			if len(row.Definition) > 40 {
				definitionDisplay = row.Definition[0:37] + "..."
			}

			err := table.Append([]string{
				row.Schema,
				row.Table,
				row.ForeignKey,
				definitionDisplay,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
			}
		}
		if err := table.Render(); err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
		}

		fmt.Println(strings.Repeat("-", 80))
		fmt.Println("* Tip: Indexes on FKs are crucial for CASCADE DELETE performance and avoiding locking issues.")
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("PostgreSQL does NOT automatically create indexes on Foreign Keys.")
	fmt.Println("While an index is not strictly required for the constraint to work,")
	fmt.Println("it is highly recommended for performance and locking reasons.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Locking: When you DELETE/UPDATE a row in the parent table, Postgres must check")
	fmt.Println("  the child table to ensure referential integrity. Without an index, this often")
	fmt.Println("  requires locking the ENTIRE child table, blocking other transactions.")
	fmt.Println("• Performance: Deletes on parent become slow (Sequential Scan on child).")
	fmt.Println("• Action: Create an index on the Foreign Key column(s) in the child table.")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema})
}
