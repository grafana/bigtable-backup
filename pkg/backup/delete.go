package backup

import (
	"context"
	"fmt"

	storageV1 "google.golang.org/api/storage/v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

// DeleteBackupConfig has the config to delete the backups.
type DeleteBackupConfig struct {
	BigtableTableID string
	BackupPath      string
	BackupTimestamp string
}

// RegisterDeleteBackupsFlags registers the flags for DeleteBackup command.
func RegisterDeleteBackupsFlags(cmd *kingpin.CmdClause) *DeleteBackupConfig {
	config := DeleteBackupConfig{}
	cmd.Flag("bigtable-table-id", "ID of the bigtable table to delete its backup").Required().StringVar(&config.BigtableTableID)
	cmd.Flag("backup-path", "GCS path where backups can be found").Required().StringVar(&config.BackupPath)
	cmd.Flag("backup-timestamp", "Timestamp of the backup to delete").Required().StringVar(&config.BackupTimestamp)
	return &config
}

// DeleteBackup deletes the backups.
func DeleteBackup(config *DeleteBackupConfig) error {
	ctx := context.Background()
	service, err := storageV1.NewService(ctx)
	if err != nil {
		return err
	}

	bucketName, objectPrefix := getBucketNameAndObjectPrefix(config.BackupPath)
	objectName := objectPrefix + config.BigtableTableID + "/" + config.BackupTimestamp + "/"

	objectListCall := service.Objects.List(bucketName)
	if objectPrefix != "" {
		objectListCall.Prefix(objectName)
	}

	objects, err := objectListCall.Do()
	if err != nil {
		return err
	}

	for _, object := range objects.Items {
		err := service.Objects.Delete(bucketName, object.Name).Do()
		if err != nil {
			return err
		}
	}

	fmt.Printf("Backup deleted for table %s with timestamp %s\n", config.BigtableTableID, config.BackupTimestamp)

	return nil
}
