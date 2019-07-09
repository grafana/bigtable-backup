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
	BigtableProjectId     string
	BigtableInstanceId    string
	BigtableTableIdPrefix string
	DestinationPath       string
	TempPrefix            string
}

func RegisterCreateBackupFlags(cmd *kingpin.CmdClause) *CreateBackupConfig {
	config := CreateBackupConfig{}
	cmd.Flag("bigtable-project-id", "The ID of the GCP project of the Cloud Bigtable instance that you want to read data from").Required().StringVar(&config.BigtableProjectId)
	cmd.Flag("bigtable-instance-id", "The ID of the Cloud Bigtable instance that contains the table").Required().StringVar(&config.BigtableInstanceId)
	cmd.Flag("bigtable-table-id-prefix",
		"Prefix to find the IDs of the Cloud Bigtable table to export. " +
		"It can be a table name to backup specific table or prefix to backup all tables matching the prefix.").Required().StringVar(&config.BigtableTableIdPrefix)
	cmd.Flag("destination-path", "GCS path where data should be written. For example, \"gs://mybucket/somefolder/\"").Required().StringVar(&config.DestinationPath)
	cmd.Flag("temp-prefix", "Path and filename prefix for writing temporary files. ex: gs://MyBucket/tmp").Required().StringVar(&config.TempPrefix)

	return &config
}

func CreateBackup(config *CreateBackupConfig) error {
	if strings.HasSuffix(config.DestinationPath, "/") {
		config.DestinationPath = config.DestinationPath[0 : len(config.DestinationPath)-1]
	}
	unixNow := time.Now().Unix()

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
		destinationPathWithTimestamp := fmt.Sprintf("%s/%s/%d/", config.DestinationPath, bigtableID, unixNow)
		createJobFromTemplateRequest := dataflowV1b3.CreateJobFromTemplateRequest{
			JobName: jobName,
			GcsPath: bigtableToGCSSequenceFileTemplatePath,
			Parameters: map[string]string{
				"bigtableProject":    config.BigtableProjectId,
				"bigtableInstanceId": config.BigtableInstanceId,
				"bigtableTableId":    bigtableID,
				"destinationPath":    destinationPathWithTimestamp,
				"filenamePrefix":     bigtableID + bigtableIDSeparatorInSeqFileName,
			},
			Environment: &dataflowV1b3.RuntimeEnvironment{
				TempLocation: config.TempPrefix,
			},
		}
		_, err = service.Projects.Templates.Create(config.BigtableProjectId, &createJobFromTemplateRequest).Do()
		if err != nil {
			return fmt.Errorf("Error backing up table with Id %s with error: %s", bigtableID, err)
		}
		fmt.Printf("Created job for backing up %s with timestamp %d\n", bigtableID, unixNow)
	}

	return nil
}

func listBigtableIDsWithPrefix(config *CreateBackupConfig) ([]string, error) {
	ctx := context.Background()
	service, err := bigtableAdminV2.NewService(ctx)
	if err != nil {
		return nil, err
	}

	parent := "projects/" + config.BigtableProjectId + "/instances/" + config.BigtableInstanceId
	listTableResponse, err := service.Projects.Instances.Tables.List(parent).Do()
	if err != nil {
		return nil, err
	}

	tableIDs := make([]string, 0, len(listTableResponse.Tables))
	tableID := ""
	for _, table := range listTableResponse.Tables {
		tableID = table.Name[strings.LastIndex(table.Name, "/")+1:]
		if strings.HasPrefix(tableID, config.BigtableTableIdPrefix) {
			tableIDs = append(tableIDs, tableID)
		}
	}

	return tableIDs, nil
}
