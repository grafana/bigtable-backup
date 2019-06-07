package main

import (
	"context"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	dataflowV1b3 "google.golang.org/api/dataflow/v1b3"
	"strings"
)

const GCSSequenceFileToBigtableTemplatePath = "gs://dataflow-templates/latest/GCS_SequenceFile_to_Cloud_Bigtable"

type restoreBackupConfig struct {
	backupPath       *string
	bigtableProjectId  *string
	bigtableInstanceId *string
	bigtableTableId    *string
	tempPrefix         *string
	backupTimestamp  *int64
}

func registerRestoreBackupsFlags(cmd *kingpin.CmdClause) (config restoreBackupConfig) {
	config.backupPath = cmd.Flag("backup-path", "GCS path where backups can be found").Required().String()
	config.bigtableProjectId = cmd.Flag("bigtable-project-id", "The ID of the GCP project of the Cloud Bigtable instance that you want to read data from").Required().String()
	config.bigtableInstanceId = cmd.Flag("bigtable-instance-id", "The ID of the Cloud Bigtable instance that contains the table").Required().String()
	config.bigtableTableId = cmd.Flag("bigtable-table-id", "ID of the Cloud Bigtable table to restore").Required().String()
	config.tempPrefix = cmd.Flag("temp-prefix", "Path and filename prefix for writing temporary files. ex: gs://MyBucket/tmp").Required().String()
	config.backupTimestamp = cmd.Flag("backup-timestamp", "Timestamp of the backup to be restored. If not set, most recent backup would be restored").Int64()
	return
}

func restoreBackup(config restoreBackupConfig) error {
	if *config.backupTimestamp == 0 {
		var err error
		config.backupTimestamp, err = getNewestBackupTimestamp(*config.backupPath)
		if err != nil {
			return err
		}
	}

	if !strings.HasPrefix(*config.backupPath, "gs://") {
		*config.backupPath = "gs://" + *config.backupPath
	}

	if !strings.HasSuffix(*config.backupPath, "/") {
		*config.backupPath += "/"
	}

	ctx := context.Background()
	service, err := dataflowV1b3.NewService(ctx)
	if err != nil {
		return err
	}

	jobName := fmt.Sprintf("import-%s-%d", *config.bigtableTableId, *config.backupTimestamp)
	restoreJobFromTemplateRequest := dataflowV1b3.CreateJobFromTemplateRequest{
		JobName: jobName,
		GcsPath: GCSSequenceFileToBigtableTemplatePath,
		Parameters: map[string]string{
			"bigtableProject":    *config.bigtableProjectId,
			"bigtableInstanceId": *config.bigtableInstanceId,
			"bigtableTableId":    *config.bigtableTableId,
			"sourcePattern":     fmt.Sprintf("%s%d/%s%s*", *config.backupPath, *config.backupTimestamp, *config.bigtableTableId, bigtableIDSeparatorInSeqFileName),
		},
		Environment: &dataflowV1b3.RuntimeEnvironment{
			TempLocation: *config.tempPrefix,
		},
	}

	_, err = service.Projects.Templates.Create(*config.bigtableProjectId, &restoreJobFromTemplateRequest).Do()
	return err
}
