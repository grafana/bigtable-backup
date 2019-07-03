package backup

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	PeriodicTableDuration time.Duration
}

func RegisterCreateBackupFlags(cmd *kingpin.CmdClause) *CreateBackupConfig {
	config := CreateBackupConfig{}
	cmd.Flag("bigtable-project-id", "The ID of the GCP project of the Cloud Bigtable instance that you want to read data from").Required().StringVar(&config.BigtableProjectId)
	cmd.Flag("bigtable-instance-id", "The ID of the Cloud Bigtable instance that contains the table").Required().StringVar(&config.BigtableInstanceId)
	cmd.Flag("bigtable-table-id-prefix", "Prefix to find the IDs of the Cloud Bigtable table to export").Required().StringVar(&config.BigtableTableIdPrefix)
	cmd.Flag("destination-path", "GCS path where data should be written. For example, \"gs://mybucket/somefolder/\"").Required().StringVar(&config.DestinationPath)
	cmd.Flag("temp-prefix", "Path and filename prefix for writing temporary files. ex: gs://MyBucket/tmp").Required().StringVar(&config.TempPrefix)
	cmd.Flag("periodic-table-duration", "Periodic config set for cortex/loki tables. Used for backing up currently active periodic table").Default("0s").DurationVar(&config.PeriodicTableDuration)

	return &config
}

func CreateBackup(config *CreateBackupConfig) error {
	if !strings.HasSuffix(config.DestinationPath, "/") {
		config.DestinationPath = config.DestinationPath + "/"
	}
	unixNow := time.Now().Unix()
	destinationPathWithTimestamp := fmt.Sprintf("%s%d/", config.DestinationPath, unixNow)

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

	bigtableTableIdPrefix := config.BigtableTableIdPrefix
	if config.PeriodicTableDuration != 0 {
		periodSecs := int64(config.PeriodicTableDuration / time.Second)
		bigtableTableIdPrefix += strconv.Itoa(int(time.Now().Unix() / periodSecs))
	}

	tableIDs := make([]string, 0, len(listTableResponse.Tables))
	tableID := ""
	for _, table := range listTableResponse.Tables {
		tableID = table.Name[strings.LastIndex(table.Name, "/")+1:]
		if strings.HasPrefix(tableID, bigtableTableIdPrefix) {
			tableIDs = append(tableIDs, tableID)
		}
	}

	return tableIDs, nil
}
