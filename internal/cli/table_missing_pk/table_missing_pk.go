package table_missing_pk

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
		Schema: "*",

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "table",

		Use: "table:missing-pk [db_name]",

		Short: "Validate tables has primary key",

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

type tableMissingPkRow struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	SizeHuman string `json:"size_human"`
	SizeBytes int64  `json:"size_bytes"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       SELECT
          n.nspname AS schema_name,
          c.relname AS table_name,
          pg_size_pretty(pg_table_size(c.oid)) AS size_human,
          pg_table_size(c.oid) AS size_bytes
       FROM pg_class AS c
       JOIN pg_namespace AS n
         ON n.oid = c.relnamespace
       WHERE 
          ($1 = '*' OR n.nspname = $1)
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%'
          AND c.relkind = 'r' -- Only ordinary tables (not views, sequences, etc.)
          AND NOT EXISTS (
             SELECT 1
             FROM pg_index AS i
             WHERE i.indrelid = c.oid
             AND i.indisprimary = 't' -- Check for Primary Key index
          )
       ORDER BY size_bytes DESC;
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

	var results []tableMissingPkRow

	for rows.Next() {
		var r tableMissingPkRow

		err := rows.Scan(
			&r.Schema,
			&r.Table,
			&r.SizeHuman,
			&r.SizeBytes,
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

		fmt.Printf("Searching for tables without PRIMARY KEY in `%s`\n", opts.DbName)
		fmt.Printf("Schema: %s\n", schemaDisplay)
		fmt.Println(strings.Repeat("-", 60))

		if len(results) == 0 {
			fmt.Println("Great! All tables have a Primary Key.")
		} else {
			table := tablewriter.NewWriter(os.Stdout)
			table.Header([]string{"Schema", "Table", "Size"})

			for _, row := range results {
				err := table.Append([]string{
					row.Schema,
					row.Table,
					row.SizeHuman,
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
				}
			}
			if err := table.Render(); err != nil {
				fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
			}
		}

		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("* Tables without PK cause replication issues and data integrity risks.")
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("Every table in a relational database should generally have a Primary Key (PK).")
	fmt.Println("A PK uniquely identifies each row and ensures data integrity.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Missing PK: Allows duplicate rows, making specific row updates/deletes difficult or impossible.")
	fmt.Println("• Replication: Many replication tools (like logical replication) REQUIRE a PK to function.")
	fmt.Println("• Action: Add a PRIMARY KEY constraint (e.g., on an ID serial/uuid column).")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema})
}
