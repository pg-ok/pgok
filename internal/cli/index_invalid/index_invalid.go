package index_invalid

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

		Use: "index:invalid [db_name]",

		Short: "Find invalid/broken indexes that failed to build",

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

type invalidRow struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	IndexName string `json:"index_name"`
	Status    string `json:"status"`
	IsValid   bool   `json:"is_valid"`
	IsReady   bool   `json:"is_ready"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       SELECT
          n.nspname AS schema_name,
          t.relname AS table_name,
          i.relname AS index_name,
          ix.indisvalid AS is_valid,
          ix.indisready AS is_ready
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
       ORDER BY n.nspname, t.relname, i.relname;
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

	var results []invalidRow

	for rows.Next() {
		var schemaName string
		var tableName string
		var indexName string
		var isValid bool
		var isReady bool

		err := rows.Scan(
			&schemaName,
			&tableName,
			&indexName,
			&isValid,
			&isReady,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Row scan failed: %v\n", err)
			os.Exit(1)
		}

		isOk := isValid && isReady

		// Filter: Skip healthy indexes
		if isOk {
			continue
		}

		results = append(results, invalidRow{
			Schema:    schemaName,
			TableName: tableName,
			IndexName: indexName,
			Status:    "Broken",
			IsValid:   isValid,
			IsReady:   isReady,
		})
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

		fmt.Printf("Validating indexes in `%s`\n", opts.DbName)
		fmt.Printf("Schema: %s\n", schemaDisplay)

		if len(results) == 0 {
			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("No broken indexes found. Everything looks good! ✨")
			fmt.Println(strings.Repeat("-", 80))
		} else {
			table := tablewriter.NewWriter(os.Stdout)
			table.Header([]string{"Schema", "Table", "Index", "Status", "Valid", "Ready"})

			for _, row := range results {
				err := table.Append([]string{
					row.Schema,
					row.TableName,
					row.IndexName,
					row.Status,
					fmt.Sprintf("%v", row.IsValid),
					fmt.Sprintf("%v", row.IsReady),
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
				}
			}
			if err := table.Render(); err != nil {
				fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
			}

			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("* Recommendation: Drop these indexes and REINDEX CONCURRENTLY.")
		}
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("Indexes typically become 'invalid' when a CREATE INDEX CONCURRENTLY operation")
	fmt.Println("fails (e.g., deadlock, unique violation) or is interrupted.")
	fmt.Println("PostgreSQL does not automatically clean them up.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Invalid indexes CANNOT be used by queries (reads).")
	fmt.Println("• However, they ARE updated by INSERT/UPDATE/DELETE (writes).")
	fmt.Println("• Result: You pay the performance cost of maintaining the index but get zero benefit.")
	fmt.Println("• Action: DROP INDEX CONCURRENTLY <name>; (and then try creating it again).")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema})
}
