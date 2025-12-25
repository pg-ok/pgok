package index_unused

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
	ScanMax int64
	Explain bool
	Output  util.OutputFormat
}

func NewCommand() *cobra.Command {
	opts := &Options{
		// Default to scanning all schemas
		Schema: "*",

		ScanMax: 0,

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "index",

		Use: "index:unused [db_name]",

		Short: "Find indexes that have scans count lower than defined",

		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			opts.DbName = args[0]
			run(opts)
		},
	}

	flags := command.Flags()
	flags.StringVar(&opts.Schema, "schema", opts.Schema, "Schema name (use '*' for all user schemas)")
	flags.Int64Var(&opts.ScanMax, "scan-count-max", opts.ScanMax, "Maximum scans count")
	flags.BoolVar(&opts.Explain, "explain", false, "Print the SQL query and explain the logic/interpretation")

	flags.Var(&opts.Output, "output", "Output format (table, json)")
	_ = command.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json"}, cobra.ShellCompDirectiveDefault
	})

	return command
}

type unusedIndexRow struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Index  string `json:"index"`
	Scans  int64  `json:"scans"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       SELECT
          s.schemaname AS schema_name,
          s.relname AS table_name,
          s.indexrelname AS index_name,
          s.idx_scan AS scans_count
       FROM pg_stat_user_indexes AS s
       JOIN pg_index AS i
         ON s.indexrelid = i.indexrelid
       WHERE 
          ($1 = '*' OR s.schemaname = $1)
          -- pg_stat_user_indexes already excludes system schemas, but we keep this for consistency
          AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
          AND s.schemaname NOT LIKE 'pg_toast%'
          AND s.idx_scan <= $2
          AND i.indisprimary = false
       ORDER BY s.schemaname, s.relname, s.idx_scan;
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

	rows, err := conn.Query(ctx, sqlQuery, opts.Schema, opts.ScanMax)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []unusedIndexRow

	for rows.Next() {
		var r unusedIndexRow

		err := rows.Scan(
			&r.Schema,
			&r.Table,
			&r.Index,
			&r.Scans,
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

		fmt.Printf("Searching for unused indexes in database `%s`\n", opts.DbName)
		fmt.Printf("Schema: %s, Max Scans: <= %d\n", schemaDisplay, opts.ScanMax)

		if len(results) == 0 {
			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("No unused indexes found within the specified criteria.")
			fmt.Println(strings.Repeat("-", 80))
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Schema", "Scans", "Table", "Index"})

		for _, row := range results {
			err := table.Append([]string{
				row.Schema,
				fmt.Sprintf("%d", row.Scans),
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

		fmt.Println(strings.Repeat("-", 80))
		fmt.Println("* Primary Keys are automatically excluded.")
		fmt.Println("* Be careful! An index might be used only once a month (e.g. for reports).")
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("Every index imposes a penalty on write operations (INSERT, UPDATE, DELETE).")
	fmt.Println("If an index is never used for reading (scans = 0), it is pure overhead.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Scans: 0 means the index has NEVER been used since statistics were last reset.")
	fmt.Println("• Action: DROP the index to speed up writes and save disk space.")
	fmt.Println("• Caution: UNIQUE indexes might have 0 scans but are required for integrity constraints.")
	fmt.Println("           Also, ensure the index isn't used only for rare (e.g., quarterly) reports.")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema, opts.ScanMax})
}
