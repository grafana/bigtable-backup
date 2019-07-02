package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	bigtableAdminV2 "google.golang.org/api/bigtableadmin/v2"
	dataflowV1b3 "google.golang.org/api/dataflow/v1b3"
)

const (
	bigtableToGCSSequenceFileTemplatePath = "gs://dataflow-templates/latest/Cloud_Bigtable_to_GCS_SequenceFile"
	bigtableIDSeparatorInSeqFileName      = ":"
)

type CreateBackupConfig struct {
	bigtableProjectId     string
	bigtableInstanceId    string
	bigtableTableIdPrefix string
	destinationPath       string
	tempPrefix            string
}

func RegisterCreateBackupFlags(cmd *kingpin.CmdClause) *CreateBackupConfig {
	config := CreateBackupConfig{}
	cmd.Flag("bigtable-project-id", "The ID of the GCP project of the Cloud Bigtable instance that you want to read data from").Required().StringVar(&config.bigtableProjectId)
	cmd.Flag("bigtable-instance-id", "The ID of the Cloud Bigtable instance that contains the table").Required().StringVar(&config.bigtableInstanceId)
	cmd.Flag("bigtable-table-id-prefix", "Prefix to find the IDs of the Cloud Bigtable table to export").Required().StringVar(&config.bigtableTableIdPrefix)
	cmd.Flag("destination-path", "GCS path where data should be written. For example, \"gs://mybucket/somefolder/\"").Required().StringVar(&config.destinationPath)
	cmd.Flag("temp-prefix", "Path and filename prefix for writing temporary files. ex: gs://MyBucket/tmp").Required().StringVar(&config.tempPrefix)

	return &config
}

func CreateBackup(config *CreateBackupConfig) error {
	if !strings.HasSuffix(config.destinationPath, "/") {
		config.destinationPath = config.destinationPath + "/"
	}
	unixNow := time.Now().Unix()
	destinationPathWithTimestamp := fmt.Sprintf("%s%d/", config.destinationPath, unixNow)

	bigtableIDs, err := listBigtableIDsWithPrefix(config)
	if err != nil {
		return err
	}

	if len(bigtableIDs) == 0 {
		return errors.New("No tables found")
	}

	ctx := context.Background()
	service, err := dataflowV1b3.NewService(ctx)
	if err != nil {
		return err
	}

	for _, bigtableID := range bigtableIDs {
		jobName := fmt.Sprintf("export-%s-%d", bigtableID, unixNow)
		createJobFromTemplateRequest := dataflowV1b3.CreateJobFromTemplateRequest{
			JobName: jobName,
			GcsPath: bigtableToGCSSequenceFileTemplatePath,
			Parameters: map[string]string{
				"bigtableProject":    config.bigtableProjectId,
				"bigtableInstanceId": config.bigtableInstanceId,
				"bigtableTableId":    bigtableID,
				"destinationPath":    destinationPathWithTimestamp,
				"filenamePrefix":     bigtableID + bigtableIDSeparatorInSeqFileName,
			},
			Environment: &dataflowV1b3.RuntimeEnvironment{
				TempLocation: config.tempPrefix,
			},
		}
		_, err = service.Projects.Templates.Create(config.bigtableProjectId, &createJobFromTemplateRequest).Do()
		if err != nil {
			return fmt.Errorf("Error backing up table with Id %s with error: %s", bigtableID, err)
		}
	}
	return nil
}

func listBigtableIDsWithPrefix(config *CreateBackupConfig) ([]string, error) {
	ctx := context.Background()
	service, err := bigtableAdminV2.NewService(ctx)
	if err != nil {
		return nil, err
	}

	parent := "projects/" + config.bigtableProjectId + "/instances/" + config.bigtableInstanceId
	listTableResponse, err := service.Projects.Instances.Tables.List(parent).Do()
	if err != nil {
		return nil, err
	}

	tableIDs := make([]string, 0, len(listTableResponse.Tables))
	tableID := ""
	for _, table := range listTableResponse.Tables {
		tableID = table.Name[strings.LastIndex(table.Name, "/")+1:]
		if strings.HasPrefix(tableID, config.bigtableTableIdPrefix) {
			tableIDs = append(tableIDs, tableID)
		}
	}

	return tableIDs, nil
}
