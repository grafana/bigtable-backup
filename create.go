package main

import (
	"context"
	"errors"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"strings"
	"time"

	bigtableAdminV2 "google.golang.org/api/bigtableadmin/v2"
	dataflowV1b3 "google.golang.org/api/dataflow/v1b3"
)

const (
	bigtableToGCSSequenceFileTemplatePath = "gs://dataflow-templates/latest/Cloud_Bigtable_to_GCS_SequenceFile"
	bigtableIDSeparatorInSeqFileName = ":"
)

type createBackupConfig struct {
	bigtableProjectId *string
	bigtableInstanceId *string
	bigtableTableIdPrefix *string
	destinationPath *string
	tempPrefix *string
}

func registerCreateBackupFlags(cmd *kingpin.CmdClause) (config createBackupConfig) {
	config.bigtableProjectId = cmd.Flag("bigtable-project-id", "The ID of the GCP project of the Cloud Bigtable instance that you want to read data from").Required().String()
	config.bigtableInstanceId = cmd.Flag("bigtable-instance-id", "The ID of the Cloud Bigtable instance that contains the table").Required().String()
	config.bigtableTableIdPrefix = cmd.Flag("bigtable-table-id-prefix", "Prefix to find the IDs of the Cloud Bigtable table to export").Required().String()
	config.destinationPath = cmd.Flag("destination-path", "GCS path where data should be written. For example, \"gs://mybucket/somefolder/\"").Required().String()
	config.tempPrefix = cmd.Flag("temp-prefix", "Path and filename prefix for writing temporary files. ex: gs://MyBucket/tmp").Required().String()
	return
}

func createBackup(config createBackupConfig) error {
	if !strings.HasSuffix(*config.destinationPath, "/") {
		*config.destinationPath = *config.destinationPath + "/"
	}
	unixNow := time.Now().Unix()
	destinationPathWithTimestamp := fmt.Sprintf("%s%d/",*config.destinationPath, unixNow)

	fmt.Println(config)
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
				"bigtableProject":    *config.bigtableProjectId,
				"bigtableInstanceId": *config.bigtableInstanceId,
				"bigtableTableId":    bigtableID,
				"destinationPath":    destinationPathWithTimestamp,
				"filenamePrefix":     bigtableID + bigtableIDSeparatorInSeqFileName,
			},
			Environment: &dataflowV1b3.RuntimeEnvironment{
				TempLocation: *config.tempPrefix,
			},
		}
		_, err = service.Projects.Templates.Create(*config.bigtableProjectId, &createJobFromTemplateRequest).Do()
		if err != nil {
			return fmt.Errorf("Error restoring table with Id %s with error: %s", bigtableID, err)
		}
	}
	return nil
}

func listBigtableIDsWithPrefix(config createBackupConfig) ([]string, error) {
	ctx := context.Background()
	service, err := bigtableAdminV2.NewService(ctx)
	if err != nil {
		return nil, err
	}

	parent := "projects/" + *config.bigtableProjectId + "/instances/" + *config.bigtableInstanceId
	listTableResponse, err := service.Projects.Instances.Tables.List(parent).Do()
	if err != nil {
		return nil, err
	}

	tableIDs := make([]string, 0, len(listTableResponse.Tables))
	tableID := ""
	for _, table := range listTableResponse.Tables {
		tableID = table.Name[strings.LastIndex(table.Name, "/")+1:]
		if strings.HasPrefix(tableID, *config.bigtableTableIdPrefix) {
			tableIDs = append(tableIDs, tableID)
		}
	}

	return tableIDs, nil
}
