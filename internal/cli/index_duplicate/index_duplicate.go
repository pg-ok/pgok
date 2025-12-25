package index_duplicate

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

		Use: "index:duplicate [db_name]",

		Short: "Find duplicate indexes (same definition) that waste space",

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

type duplicateRow struct {
	Schema      string   `json:"schema"`
	SizeHuman   string   `json:"size_human"`
	SizeBytes   int64    `json:"size_bytes"`
	KeepIndex   string   `json:"keep_index"`
	DropIndexes []string `json:"drop_indexes"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       SELECT
          schema_name,
          PG_SIZE_PRETTY(SUM(PG_RELATION_SIZE(idx))::BIGINT) AS size_human,
          SUM(PG_RELATION_SIZE(idx))::BIGINT AS size_bytes,
          (ARRAY_AGG(idx::REGCLASS::TEXT))[1] AS index1,
          (ARRAY_AGG(idx::REGCLASS::TEXT))[2] AS index2,
          (ARRAY_AGG(idx::REGCLASS::TEXT))[3] AS index3,
          (ARRAY_AGG(idx::REGCLASS::TEXT))[4] AS index4
       FROM (
          SELECT
             n.nspname AS schema_name,
             indexrelid AS idx,
             (
                indrelid::TEXT || E'\n' ||
                indclass::TEXT || E'\n' ||
                indkey::TEXT || E'\n' ||
                indoption::TEXT || E'\n' ||
                COALESCE(indexprs::TEXT, '') || E'\n' ||
                COALESCE(indpred::TEXT, '')
             ) AS key
          FROM pg_index AS i
          JOIN pg_class AS c
            ON c.oid = i.indexrelid
          JOIN pg_namespace AS n
            ON n.oid = c.relnamespace
          WHERE 
            ($1 = '*' OR n.nspname = $1)
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname NOT LIKE 'pg_toast%'
       ) sub
       GROUP BY schema_name, sub.key 
       HAVING COUNT(*) > 1
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

	var results []duplicateRow

	for rows.Next() {
		var r duplicateRow
		// Используем указатели, так как 2, 3 и 4 индексы могут быть NULL
		var idx1, idx2, idx3, idx4 *string

		err := rows.Scan(
			&r.Schema,
			&r.SizeHuman,
			&r.SizeBytes,
			&idx1,
			&idx2,
			&idx3,
			&idx4,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Row scan failed: %v\n", err)
			os.Exit(1)
		}

		// Logic: keep the first found index, suggest dropping the rest
		if idx1 != nil {
			r.KeepIndex = *idx1
		}

		r.DropIndexes = []string{}
		if idx2 != nil {
			r.DropIndexes = append(r.DropIndexes, *idx2)
		}
		if idx3 != nil {
			r.DropIndexes = append(r.DropIndexes, *idx3)
		}
		if idx4 != nil {
			r.DropIndexes = append(r.DropIndexes, *idx4)
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
		fmt.Printf("Searching for DUPLICATE indexes in `%s`\n", opts.DbName)

		schemaDisplay := opts.Schema
		if opts.Schema == "*" {
			schemaDisplay = "ALL (except system)"
		}
		fmt.Printf("Schema: %s\n", schemaDisplay)

		if len(results) == 0 {
			fmt.Println("\nNo duplicate indexes found. Good job!")
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Schema", "Wasted Size", "Keep Index", "Drop Duplicate(s)"})

		for _, row := range results {
			dropList := strings.Join(row.DropIndexes, ", ")
			err := table.Append([]string{
				row.Schema,
				row.SizeHuman,
				row.KeepIndex,
				dropList,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
			}
		}
		if err := table.Render(); err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
		}

		fmt.Println(strings.Repeat("-", 80))
		fmt.Println("* Warning: The 'Keep' index is simply the first one found.")
		fmt.Println("* Check if one name follows your naming convention better than the others before dropping.")
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("PostgreSQL allows creating multiple indexes with the EXACT same definition")
	fmt.Println("(same columns, same order, same partial condition).")
	fmt.Println("This often happens when migrations are applied incorrectly or developers")
	fmt.Println("don't realize an index already exists.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Duplicate indexes are pure overhead.")
	fmt.Println("• They double the maintenance cost for INSERT/UPDATE/DELETE.")
	fmt.Println("• They take up disk space and RAM (buffer cache) for no benefit.")
	fmt.Println("• Action: You should safely DROP the duplicates and keep one.")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema})
}
