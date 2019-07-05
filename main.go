package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/grafana/bigtable-backup/pkg/backup"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("bigtable-backup", "A command-line for creating and restoring backups from bigtable.")

	createCmd      = app.Command("create", "Create backups for specific table or active periodic table or all the tables for given prefix")
	createCmdFlags = backup.RegisterCreateBackupFlags(createCmd)

	listBackupsCmd  = app.Command("list-backups", "Restore backups of all or specific bigtableTableId created for specific timestamp")
	listBackupFlags = backup.RegisterListBackupsFlags(listBackupsCmd)

	restoreCmd      = app.Command("restore", "Restore backups of specific bigtableTableId created at a timestamp")
	restoreCmdFlags = backup.RegisterRestoreBackupsFlags(restoreCmd)
)

func main() {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case createCmd.FullCommand():
		if err := backup.CreateBackup(createCmdFlags); err != nil {
			log.Fatalf("Error creating backups %v", err)
		}
	case listBackupsCmd.FullCommand():
		if backups, err := backup.ListBackups(listBackupFlags); err != nil {
			log.Fatalf("Error listing backups %v", err)
		} else {
			if len(backups) == 0 {
				fmt.Println("No backups found")
			} else {
				fmt.Println("TableName: Backup Timestamps")
				for tableName, backupTimestamps := range backups {
					fmt.Printf("%s: %s\n", tableName, strings.Join(backupTimestamps, ", "))
				}
			}
		}
	case restoreCmd.FullCommand():
		if err := backup.RestoreBackup(restoreCmdFlags); err != nil {
			log.Fatalf("Error restoring backup %v", err)
		}
	}
}
