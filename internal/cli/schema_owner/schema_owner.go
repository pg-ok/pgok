package schema_owner

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
	DbName        string
	Schema        string
	ExpectedOwner string
	Explain       bool
	Output        util.OutputFormat
}

func NewCommand() *cobra.Command {
	opts := &Options{
		// Default to checking all schemas
		Schema: "*",

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "schema",

		Use: "schema:owner [db_name]",

		Short: "Detect objects owned by unexpected users (Tables, Enums, Sequences...)",

		Long: "Lists database objects (Tables, Views, Sequences, Enums, Domains) that are NOT owned by the specified user.",

		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			opts.DbName = args[0]
			run(opts)
		},
	}

	flags := command.Flags()

	flags.StringVar(&opts.ExpectedOwner, "expected", "", "The username that SHOULD own the objects")
	_ = command.MarkFlagRequired("expected")

	flags.StringVar(&opts.Schema, "schema", opts.Schema, "Schema name (use '*' for all user schemas)")
	flags.BoolVar(&opts.Explain, "explain", false, "Print the SQL query and explain the logic/interpretation")

	flags.Var(&opts.Output, "output", "Output format (table, json)")
	_ = command.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json"}, cobra.ShellCompDirectiveDefault
	})

	return command
}

type ownerRow struct {
	SchemaName  string `json:"schema_name"`
	ObjectName  string `json:"object_name"`
	ObjectType  string `json:"object_type"`
	ActualOwner string `json:"actual_owner"`
	FixCommand  string `json:"fix_command"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	// Union pg_class (tables/views/seqs) and pg_type (enums/domains)
	rawSql := `
       SELECT schema_name, object_name, object_type, actual_owner
       FROM (
          -- 1. Relations (Tables, Sequences, Views, MatViews)
          SELECT
             c.relname AS object_name,
             CASE c.relkind
                WHEN 'r' THEN 'TABLE'
                WHEN 'v' THEN 'VIEW'
                WHEN 'm' THEN 'MATERIALIZED VIEW'
                WHEN 'S' THEN 'SEQUENCE'
                WHEN 'f' THEN 'FOREIGN TABLE'
                WHEN 'p' THEN 'PARTITIONED TABLE'
                ELSE 'UNKNOWN (' || c.relkind::text || ')'
             END AS object_type,
             r.rolname AS actual_owner,
             n.nspname AS schema_name
          FROM pg_class c
          JOIN pg_roles r ON r.oid = c.relowner
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relkind IN ('r', 'v', 'm', 'S', 'f', 'p')

          UNION ALL

          -- 2. Types (Enums, Domains)
          -- typtype: e=enum, d=domain. (b=base, c=composite skipping)
          SELECT
             t.typname AS object_name,
             CASE t.typtype
                WHEN 'e' THEN 'TYPE'   -- ENUM is handled via ALTER TYPE
                WHEN 'd' THEN 'DOMAIN' -- DOMAIN is handled via ALTER DOMAIN
                ELSE 'TYPE'
             END AS object_type,
             r.rolname AS actual_owner,
             n.nspname AS schema_name
          FROM pg_type t
          JOIN pg_roles r ON r.oid = t.typowner
          JOIN pg_namespace n ON n.oid = t.typnamespace
          WHERE t.typtype IN ('e', 'd')
       ) AS all_objects
       WHERE 
         ($1 = '*' OR schema_name = $1)
         AND schema_name NOT IN ('pg_catalog', 'information_schema')
         AND schema_name NOT LIKE 'pg_toast%'
         AND actual_owner != $2
       ORDER BY schema_name, object_type, object_name;
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

	rows, err := conn.Query(ctx, sqlQuery, opts.Schema, opts.ExpectedOwner)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []ownerRow

	for rows.Next() {
		var r ownerRow
		err := rows.Scan(&r.SchemaName, &r.ObjectName, &r.ObjectType, &r.ActualOwner)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Row scan failed: %v\n", err)
			os.Exit(1)
		}

		cmdType := r.ObjectType
		r.FixCommand = fmt.Sprintf("ALTER %s %s.%s OWNER TO %s;", cmdType, r.SchemaName, r.ObjectName, opts.ExpectedOwner)

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

		fmt.Printf("Checking schema ownership in `%s` (Expected: %s)\n", opts.DbName, opts.ExpectedOwner)
		fmt.Printf("Schema: %s\n", schemaDisplay)

		if len(results) == 0 {
			fmt.Println(strings.Repeat("-", 80))
			fmt.Printf("All objects (Tables, Types, Seqs) are correctly owned by '%s'. Good job! ✨\n", opts.ExpectedOwner)
			fmt.Println(strings.Repeat("-", 80))
			return
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Schema", "Type", "Object", "Current Owner", "Fix Command"})

		for _, row := range results {
			err := table.Append([]string{
				row.SchemaName,
				row.ObjectType,
				row.ObjectName,
				row.ActualOwner,
				row.FixCommand,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
			}
		}
		if err := table.Render(); err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
		}

		fmt.Println(strings.Repeat("-", 100))
		fmt.Println("* Mismatched owners prevent operations like VACUUM or ALTER ...")
		fmt.Println("* Run the Fix Commands above to assign ownership to the expected user.")
	}
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("Ownership issues often occur when migrations are run by different users (e.g., 'deploy' vs 'postgres').")
	fmt.Println("This prevents maintenance tasks (like VACUUM) or future ALTER operations from succeeding.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Expected: The user who SHOULD own all objects (usually the application user or migration user).")
	fmt.Println("• Actual: The user who currently owns the object.")
	fmt.Println("• Action: Run the generated REASSIGN/ALTER commands to fix ownership.")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema, opts.ExpectedOwner})
}
