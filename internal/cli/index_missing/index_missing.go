package index_missing

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
	RowsMin int64
	Explain bool
	Output  util.OutputFormat
}

func NewCommand() *cobra.Command {
	opts := &Options{
		// Default to scanning all schemas
		Schema: "*",

		RowsMin: 1000,

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "index",

		Use: "index:missing [db_name]",

		Short: "Find missing indexes based on sequential scan statistics",

		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			opts.DbName = args[0]
			run(opts)
		},
	}

	flags := command.Flags()
	flags.StringVar(&opts.Schema, "schema", opts.Schema, "Schema name (use '*' for all user schemas)")
	flags.Int64Var(&opts.RowsMin, "rows-min", opts.RowsMin, "Minimum table rows to calculate ratio (ignore small tables)")
	flags.BoolVar(&opts.Explain, "explain", false, "Print the SQL query and explain the logic/interpretation")

	flags.Var(&opts.Output, "output", "Output format (table, json)")
	_ = command.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json"}, cobra.ShellCompDirectiveDefault
	})

	return command
}

type missingIndexRow struct {
	Schema             string   `json:"schema"`
	Table              string   `json:"table"`
	SequentialScans    int64    `json:"sequential_scans"`
	IndexScans         int64    `json:"index_scans"`
	RowsReadSequential int64    `json:"rows_read_sequential"`
	TableRows          int64    `json:"table_rows"`
	Ratio              *float64 `json:"ratio"` // Pointer to handle NULL (Inf)
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       SELECT
          schemaname AS schema_name,
          relname AS table_name,
          seq_scan AS sequential_scans,
          idx_scan AS index_scans,
          seq_tup_read AS rows_read_sequential,
          n_live_tup AS table_rows,
          ROUND(
             (seq_tup_read::NUMERIC / NULLIF(idx_scan, 0)),
             2
          )::FLOAT AS ratio
       FROM pg_stat_user_tables
       WHERE 
          ($1 = '*' OR schemaname = $1)
          AND seq_scan > 0
          AND n_live_tup >= $2
       ORDER BY seq_tup_read DESC;
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

	rows, err := conn.Query(ctx, sqlQuery, opts.Schema, opts.RowsMin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []missingIndexRow

	for rows.Next() {
		var r missingIndexRow

		err := rows.Scan(
			&r.Schema,
			&r.Table,
			&r.SequentialScans,
			&r.IndexScans,
			&r.RowsReadSequential,
			&r.TableRows,
			&r.Ratio,
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

		fmt.Printf("Searching for missing indexes (high sequential scans) in `%s`\n", opts.DbName)
		fmt.Printf("Schema: %s, Rows Min: >= %d\n", schemaDisplay, opts.RowsMin)

		if len(results) == 0 {
			fmt.Println(strings.Repeat("-", 80))
			fmt.Println("No tables with high sequential scans found. Great!")
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Schema", "Table", "Ratio", "Rows Read (Seq)", "Seq Scans", "Idx Scans", "Table Rows"})

		for _, row := range results {
			ratioDisplay := "Inf"
			if row.Ratio != nil {
				val := *row.Ratio
				if val > 1000.0 {
					ratioDisplay = fmt.Sprintf("%.0f", val)
				} else {
					ratioDisplay = fmt.Sprintf("%.2f", val)
				}
			}

			err := table.Append([]string{
				row.Schema,
				row.Table,
				ratioDisplay,
				fmt.Sprintf("%d", row.RowsReadSequential),
				fmt.Sprintf("%d", row.SequentialScans),
				fmt.Sprintf("%d", row.IndexScans),
				fmt.Sprintf("%d", row.TableRows),
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
			}
		}
		if err := table.Render(); err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
		}

		fmt.Println(strings.Repeat("-", 115))
		fmt.Printf("* Hidden tables with < %d rows (Seq Scan is usually fine there).\n", opts.RowsMin)
		fmt.Println("* Ratio = Rows Read Seq / Index Scans. High ratio means we read MANY rows for every index scan (or lack thereof).")
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("When PostgreSQL cannot find a suitable index for a query, it performs a Sequential Scan")
	fmt.Println("(reading the entire table row by row). This is very expensive for large tables.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Ratio: Number of rows read by sequential scans divided by the number of index scans.")
	fmt.Println("• High Ratio (> 1000): Means we are reading MILLIONS of rows via Seq Scan compared to Index Scans.")
	fmt.Println("• Action: Look at slow queries filtering on this table and add indexes on the columns used in WHERE.")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema, opts.RowsMin})
}
