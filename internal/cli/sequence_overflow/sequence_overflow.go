package sequence_overflow

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
	UsedMin float64
	Explain bool
	Output  util.OutputFormat
}

func NewCommand() *cobra.Command {
	opts := &Options{
		// Default to checking all schemas
		Schema: "*",

		UsedMin: 0.0,

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "sequence",

		Use: "sequence:overflow [db_name]",

		Short: "Check sequences exhaustion",

		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			opts.DbName = args[0]
			run(opts)
		},
	}

	flags := command.Flags()
	flags.StringVar(&opts.Schema, "schema", opts.Schema, "Schema name (use '*' for all user schemas)")
	flags.Float64Var(&opts.UsedMin, "used-percent-min", opts.UsedMin, "Filter sequences by minimum used percentage (e.g. 80.0)")
	flags.BoolVar(&opts.Explain, "explain", false, "Print the SQL query and explain the logic/interpretation")

	flags.Var(&opts.Output, "output", "Output format (table, json)")
	_ = command.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json"}, cobra.ShellCompDirectiveDefault
	})

	return command
}

type sequenceUsageRow struct {
	Schema      string  `json:"schema"`
	Sequence    string  `json:"sequence"`
	DataType    string  `json:"data_type"`
	UsedPercent float64 `json:"used_percent"`
	LastValue   int64   `json:"last_value"`
	MaxValue    int64   `json:"max_value"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       WITH sequence_stats AS (
          SELECT
             schemaname AS schema_name,
             sequencename AS sequence_name,
             data_type::TEXT AS data_type,
             COALESCE(last_value, 0) AS last_value, -- Handle NULL if no permissions
             max_value,
             COALESCE(ROUND(
                (COALESCE(last_value, 0)::NUMERIC / NULLIF(max_value::NUMERIC, 0)) * 100.0,
                2
             )::FLOAT, 0.0) AS percent -- Handle division by zero or NULLs
          FROM pg_sequences
          WHERE 
             ($1 = '*' OR schemaname = $1)
             AND schemaname NOT IN ('pg_catalog', 'information_schema')
             AND schemaname NOT LIKE 'pg_toast%'
       )
       SELECT *
       FROM sequence_stats
       WHERE percent >= $2
       ORDER BY percent DESC;
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

	rows, err := conn.Query(ctx, sqlQuery, opts.Schema, opts.UsedMin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []sequenceUsageRow

	for rows.Next() {
		var r sequenceUsageRow

		err := rows.Scan(
			&r.Schema,
			&r.Sequence,
			&r.DataType,
			&r.LastValue,
			&r.MaxValue,
			&r.UsedPercent,
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

		fmt.Printf("Checking sequence usage in `%s`\n", opts.DbName)
		fmt.Printf("Schema: %s\n", schemaDisplay)

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Schema", "Sequence", "Type", "Used % (Current / Max)"})

		for _, row := range results {
			usedPercentDisplay := fmt.Sprintf("%.2f%%", row.UsedPercent)
			if row.UsedPercent > 80.0 {
				usedPercentDisplay += " [!]"
			}

			usageDisplay := fmt.Sprintf(
				"%s (%d / %d)",
				usedPercentDisplay, row.LastValue, row.MaxValue,
			)

			err := table.Append([]string{
				row.Schema,
				row.Sequence,
				row.DataType,
				usageDisplay,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
			}
		}
		if err := table.Render(); err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
		}

		fmt.Println(strings.Repeat("-", 115))
		fmt.Println("* [!] indicates sequences nearing exhaustion (>80%). INT overflow risk!")
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("Sequences in PostgreSQL have maximum limits (e.g., 2.1B for INTEGER).")
	fmt.Println("If a sequence hits this limit, INSERTs will fail, causing downtime.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Used %: How close the sequence is to its MAX_VALUE.")
	fmt.Println("• Risk: If > 80-90%, plan a migration to BIGINT immediately.")
	fmt.Println("• Note: 'last_value' might be approximate or require permissions to read.")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema, opts.UsedMin})
}
