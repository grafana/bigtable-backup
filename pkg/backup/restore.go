package backup

import (
	"context"
	"fmt"
	"strings"

	dataflowV1b3 "google.golang.org/api/dataflow/v1b3"
	"gopkg.in/alecthomas/kingpin.v2"
)

const GCSSequenceFileToBigtableTemplatePath = "gs://dataflow-templates/latest/GCS_SequenceFile_to_Cloud_Bigtable"

type RestoreBackupConfig struct {
	BackupPath         string
	BigtableProjectId  string
	BigtableInstanceId string
	BigtableTableId    string
	TempPrefix         string
	BackupTimestamp    int64
}

func RegisterRestoreBackupsFlags(cmd *kingpin.CmdClause) *RestoreBackupConfig {
	config := RestoreBackupConfig{}
	cmd.Flag("backup-path", "GCS path where backups can be found").Required().StringVar(&config.BackupPath)
	cmd.Flag("bigtable-project-id", "The ID of the GCP project of the Cloud Bigtable instance that you want to read data from").Required().StringVar(&config.BigtableProjectId)
	cmd.Flag("bigtable-instance-id", "The ID of the Cloud Bigtable instance that contains the table").Required().StringVar(&config.BigtableInstanceId)
	cmd.Flag("bigtable-table-id", "ID of the Cloud Bigtable table to restore").Required().StringVar(&config.BigtableTableId)
	cmd.Flag("temp-prefix", "Path and filename prefix for writing temporary files. ex: gs://MyBucket/tmp").Required().StringVar(&config.TempPrefix)
	cmd.Flag("backup-timestamp", "Timestamp of the backup to be restored. If not set, most recent backup would be restored").Int64Var(&config.BackupTimestamp)

	return &config
}

func RestoreBackup(config *RestoreBackupConfig) error {
	if config.BackupTimestamp == 0 {
		backupTimestamp, err := getNewestBackupTimestamp(config.BackupPath, config.BigtableTableId)
		if err != nil {
			return err
		}
		config.BackupTimestamp = *backupTimestamp
		fmt.Printf("Newest backup for %s is for timestamp %d\n", config.BigtableTableId, config.BackupTimestamp)
	}

	if !strings.HasPrefix(config.BackupPath, "gs://") {
		config.BackupPath = "gs://" + config.BackupPath
	}

	if strings.HasSuffix(config.BackupPath, "/") {
		config.BackupPath = config.BackupPath[0 : len(config.BackupPath)-1]
	}

	ctx := context.Background()
	service, err := dataflowV1b3.NewService(ctx)
	if err != nil {
		return err
	}

	jobName := fmt.Sprintf("import-%s-%d", config.BigtableTableId, config.BackupTimestamp)
	restoreJobFromTemplateRequest := dataflowV1b3.CreateJobFromTemplateRequest{
		JobName: jobName,
		GcsPath: GCSSequenceFileToBigtableTemplatePath,
		Parameters: map[string]string{
			"bigtableProject":    config.BigtableProjectId,
			"bigtableInstanceId": config.BigtableInstanceId,
			"bigtableTableId":    config.BigtableTableId,
			"sourcePattern":      fmt.Sprintf("%s/%s/%d/%s%s*", config.BackupPath, config.BigtableTableId, config.BackupTimestamp, config.BigtableTableId, bigtableIDSeparatorInSeqFileName),
		},
		Environment: &dataflowV1b3.RuntimeEnvironment{
			TempLocation: config.TempPrefix,
		},
	}

	_, err = service.Projects.Templates.Create(config.BigtableProjectId, &restoreJobFromTemplateRequest).Do()
	fmt.Printf("Created job for restoring %s with timestamp %d\n", config.BigtableTableId, config.BackupTimestamp)

	return err
}
