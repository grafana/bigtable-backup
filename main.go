package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/grafana/bigtable-backup/pkg/backup"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("bigtable-backup", "A command-line for creating and restoring backups from bigtable.")

	createCmd      = app.Command("create", "Create backups for specific table or all the tables for given prefix")
	createCmdFlags = backup.RegisterCreateBackupFlags(createCmd)

	listBackupsCmd  = app.Command("list-backups", "Restore backups of all or specific bigtableTableId created for specific timestamp")
	listBackupFlags = backup.RegisterListBackupsFlags(listBackupsCmd)

	restoreCmd      = app.Command("restore", "Restore backups of specific bigtableTableId created at a timestamp")
	restoreCmdFlags = backup.RegisterRestoreBackupsFlags(restoreCmd)

	deleteBackupsCmd  = app.Command("delete-backup", "Delete backup of a table with timestamp")
	deleteBackupFlags = backup.RegisterDeleteBackupsFlags(deleteBackupsCmd)
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
			if strings.ToLower(listBackupFlags.OutputFormat) == "json" {
				output, err := json.Marshal(backups)
				if err != nil {
					log.Fatalf("Failed to print backups in json format with error %v", err)
				}
				fmt.Printf("%s", output)
			} else {
				if len(backups) == 0 {
					fmt.Println("No backups found")
					return
				}
				fmt.Println("TableName: Backup Timestamps")
				for tableName, backupTimestamps := range backups {
					fmt.Printf("%s: %s\n", tableName, strings.Trim(strings.Replace(fmt.Sprint(backupTimestamps), " ", ",", -1), "[]"))
				}
			}
		}
	case restoreCmd.FullCommand():
		if err := backup.RestoreBackup(restoreCmdFlags); err != nil {
			log.Fatalf("Error restoring backup %v", err)
		}
	case deleteBackupsCmd.FullCommand():
		if err := backup.DeleteBackup(deleteBackupFlags); err != nil {
			log.Fatalf("Error deleting backup %v", err)
		}
	}
}
