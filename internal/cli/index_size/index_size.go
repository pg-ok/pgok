package index_size

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pg-ok/pgok/internal/db"
	"github.com/pg-ok/pgok/internal/util"

	"github.com/jackc/pgx/v5"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

type Options struct {
	DbName  string
	Schema  string
	SizeMin int64
	Explain bool
	Output  util.OutputFormat
}

func NewCommand() *cobra.Command {
	opts := &Options{
		// Default to scanning all schemas
		Schema: "*",

		SizeMin: 0,

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "index",

		Use: "index:size [db_name]",

		Short: "Show index sizes sorted by size (descending)",

		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			opts.DbName = args[0]
			run(opts)
		},
	}

	flags := command.Flags()
	flags.StringVar(&opts.Schema, "schema", opts.Schema, "Schema name (use '*' for all user schemas)")
	flags.Int64Var(&opts.SizeMin, "size-min", opts.SizeMin, "Minimum index size in bytes (exclude smaller indexes)")
	flags.BoolVar(&opts.Explain, "explain", false, "Print the SQL query and explain the logic/interpretation")

	flags.Var(&opts.Output, "output", "Output format (table, json)")
	_ = command.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json"}, cobra.ShellCompDirectiveDefault
	})

	return command
}

type indexSizeRow struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Index     string `json:"index"`
	SizeHuman string `json:"size_human"`
	SizeBytes int64  `json:"size_bytes"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       SELECT
          n.nspname AS schema_name,
          t.relname AS table_name,
          i.relname AS index_name,
          pg_size_pretty(pg_relation_size(i.oid)) AS index_size_human,
          pg_relation_size(i.oid) AS index_size_bytes
       FROM pg_class AS t
       JOIN pg_index AS ix
         ON t.oid = ix.indrelid
       JOIN pg_class AS i
         ON i.oid = ix.indexrelid
       JOIN pg_namespace AS n
         ON i.relnamespace = n.oid
       WHERE 
          ($1 = '*' OR n.nspname = $1)
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%'
          AND ix.indisprimary = false -- Excluding primary key
          AND pg_relation_size(i.oid) >= $2
       ORDER BY index_size_bytes DESC;
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

	rows, err := conn.Query(ctx, sqlQuery, opts.Schema, opts.SizeMin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []indexSizeRow

	for rows.Next() {
		var r indexSizeRow

		err := rows.Scan(
			&r.Schema,
			&r.Table,
			&r.Index,
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

		fmt.Printf("Analyzing index sizes in database `%s`\n", opts.DbName)
		fmt.Printf("Schema: %s, Size Min: >= %d bytes\n", schemaDisplay, opts.SizeMin)

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Schema", "Size", "Table", "Index"})

		for _, row := range results {
			err := table.Append([]string{
				row.Schema,
				row.SizeHuman,
				row.Table,
				row.Index,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
			}
		}
		if err := table.Render(); err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
		}
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("Indexes consume disk space and, more importantly, RAM (shared_buffers).")
	fmt.Println("Large indexes are slower to scan and harder to keep cached.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Bloat: If an index is significantly larger than the table data, it might be bloated.")
	fmt.Println("• Action: Consider REINDEX CONCURRENTLY to reclaim space and improve performance.")
	fmt.Println("• Cleanup: If a large index is also 'Unused' (check index:unused), DROP it immediately.")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema, opts.SizeMin})
}
