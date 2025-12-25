package app_db_list

import (
	"fmt"
	"os"

	"github.com/pg-ok/pgok/internal/db"

	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	command := &cobra.Command{
		GroupID: "app",

		Use: "app:db:list",

		Short: "List available databases from config",

		Run: func(cmd *cobra.Command, args []string) {
			manager := db.NewDbManager()
			names := manager.GetConfigDatabaseNames()

			fmt.Println("Configured databases:")
			for _, name := range names {
				fmt.Printf("- %s\n", name)
			}

			if len(names) == 0 {
				fmt.Fprintf(os.Stderr, "No databases found in config/pgok.toml\n")
			}
		},
	}

	return command
}
