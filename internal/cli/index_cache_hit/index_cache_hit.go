package index_cache_hit

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
	DbName   string
	Schema   string
	CallsMin int64
	Explain  bool
	Output   util.OutputFormat
}

func NewCommand() *cobra.Command {
	opts := &Options{
		// Default to scanning all schemas
		Schema: "*",

		CallsMin: 1000,

		Output: util.OutputFormatTable,
	}

	command := &cobra.Command{
		GroupID: "index",

		Use: "index:cache-hit [db_name]",

		Short: "Check index cache efficiency (Disk Reads vs RAM Hits)",

		Args: cobra.ExactArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			opts.DbName = args[0]
			run(opts)
		},
	}

	flags := command.Flags()
	flags.StringVar(&opts.Schema, "schema", opts.Schema, "Schema name (use '*' for all user schemas)")
	flags.Int64Var(&opts.CallsMin, "calls-min", opts.CallsMin, "Minimum total block accesses (hits + reads) to include")
	flags.BoolVar(&opts.Explain, "explain", false, "Print the SQL query and explain the logic/interpretation")

	flags.Var(&opts.Output, "output", "Output format (table, json)")
	_ = command.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json"}, cobra.ShellCompDirectiveDefault
	})

	return command
}

type indexType string

const (
	idxTypePK     indexType = "PK"
	idxTypeUnique indexType = "UQ"
	idxTypeNormal indexType = "IDX"
)

type cacheHitRow struct {
	Schema     string    `json:"schema"`
	Table      string    `json:"table"`
	Index      string    `json:"index"`
	IndexType  indexType `json:"index_type"` // Для JSON сериализации
	HitRatio   float64   `json:"hit_ratio"`
	DiskReads  int64     `json:"disk_reads"`
	MemoryHits int64     `json:"memory_hits"`
}

func run(opts *Options) {
	manager := db.NewDbManager()

	rawSql := `
       SELECT
          s.schemaname AS schema_name,
          relname AS table_name,
          indexrelname AS index_name,
          idx_blks_read AS disk_reads,
          idx_blks_hit AS memory_hits,
          ROUND(
             COALESCE(
                (s.idx_blks_hit::NUMERIC / NULLIF(s.idx_blks_hit + s.idx_blks_read, 0)) * 100.0,
                0.0
             ),
             2
          )::FLOAT AS hit_ratio,
          CASE
             WHEN i.indisprimary THEN 'PK'
             WHEN i.indisunique THEN 'UQ'
             ELSE 'IDX'
          END AS index_type_code
       FROM pg_statio_user_indexes AS s
       JOIN pg_index AS i
         ON s.indexrelid = i.indexrelid
       WHERE 
         ($1 = '*' OR s.schemaname = $1)
         AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
         AND s.schemaname NOT LIKE 'pg_toast%'
         
       AND (s.idx_blks_hit + s.idx_blks_read) >= $2
       ORDER BY hit_ratio ASC;
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

	rows, err := conn.Query(ctx, sqlQuery, opts.Schema, opts.CallsMin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var results []cacheHitRow

	for rows.Next() {
		var r cacheHitRow
		var typeCode string

		err := rows.Scan(
			&r.Schema,
			&r.Table,
			&r.Index,
			&r.DiskReads,
			&r.MemoryHits,
			&r.HitRatio,
			&typeCode,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Row scan failed: %v\n", err)
			os.Exit(1)
		}

		switch typeCode {
		case "PK":
			r.IndexType = idxTypePK
		case "UQ":
			r.IndexType = idxTypeUnique
		default:
			r.IndexType = idxTypeNormal
		}

		results = append(results, r)
	}

	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "Rows iteration failed: %v\n", rows.Err())
		os.Exit(1)
	}

	// Вывод результатов
	switch opts.Output {
	case util.OutputFormatJson:
		jsonData, _ := json.MarshalIndent(results, "", "  ")
		fmt.Println(string(jsonData))

	default:
		fmt.Printf("Analyzing Index Cache Hit Ratio in `%s`\n", opts.DbName)

		schemaDisplay := opts.Schema
		if opts.Schema == "*" {
			schemaDisplay = "ALL (except system)"
		}
		fmt.Printf("Schema: %s, Min Total Calls: >= %d\n", schemaDisplay, opts.CallsMin)

		table := tablewriter.NewWriter(os.Stdout)
		table.Header([]string{"Table", "Index", "Ratio %", "Disk Reads", "Mem Hits"})

		for _, row := range results {
			ratioDisplay := fmt.Sprintf("%.2f%%", row.HitRatio)

			indexDisplay := row.Index
			switch row.IndexType {
			case idxTypePK:
				indexDisplay += " [PK]"
			case idxTypeUnique:
				indexDisplay += " [UQ]"
			}

			err := table.Append([]string{
				fmt.Sprintf("%s.%s", row.Schema, row.Table),
				indexDisplay,
				ratioDisplay,
				fmt.Sprintf("%d", row.DiskReads),
				fmt.Sprintf("%d", row.MemoryHits),
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error appending table row: %v\n", err)
			}
		}
		if err := table.Render(); err != nil {
			fmt.Fprintf(os.Stderr, "Error rendering table: %v\n", err)
		}

		fmt.Println(strings.Repeat("-", 80))
		fmt.Println("* Low Ratio (< 95%) means the index is often read from DISK (slow), not RAM.")
		fmt.Println("* [PK] = Primary Key, [UQ] = Unique Index. These are critical for data integrity.")
		fmt.Printf("* Hidden indexes with total activity < %d calls.\n", opts.CallsMin)
	}
}

func (s indexType) String() string {
	return string(s)
}

func (s indexType) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s))
}

func printExplanation(sqlQuery string, opts *Options) {
	fmt.Println("📖 EXPLANATION")
	fmt.Println("-------------")
	fmt.Println("PostgreSQL attempts to keep frequently accessed index blocks in RAM (Shared Buffers).")
	fmt.Println("When data is found in RAM, it's a 'Hit'. When it must be fetched from disk, it's a 'Read'.")
	fmt.Println("Disk I/O is significantly slower than RAM access.")
	fmt.Println("")

	fmt.Println("🧠 INTERPRETATION")
	fmt.Println("-----------------")
	fmt.Println("• Ratio > 99%: Excellent. Most data is served from memory.")
	fmt.Println("• Ratio < 95%: Warning. Indexes are often read from disk. This may indicate:")
	fmt.Println("    - Insufficient RAM allocated to PostgreSQL (shared_buffers).")
	fmt.Println("    - The index is bloated (too large).")
	fmt.Println("    - Cold data is being accessed (normal for historical queries).")
	fmt.Println("")

	fmt.Println("💻 SQL QUERY")
	fmt.Println("------------")
	util.PrintRunnableSQL(sqlQuery, []interface{}{opts.Schema, opts.CallsMin})
}
