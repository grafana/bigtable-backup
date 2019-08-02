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
	jobStateCheckDuration                 = 10 * time.Second
)

// CreateBackupConfig is the config for CreateBackup command.
type CreateBackupConfig struct {
	BigtableProjectID     string
	BigtableInstanceID    string
	BigtableTableIDPrefix string
	DestinationPath       string
	TempPrefix            string
	JobLocation           string
}

// RegisterCreateBackupFlags registers the flags for CreateBackup command.
func RegisterCreateBackupFlags(cmd *kingpin.CmdClause) *CreateBackupConfig {
	config := CreateBackupConfig{}
	cmd.Flag("bigtable-project-id", "The ID of the GCP project of the Cloud Bigtable instance that you want to read data from").Required().StringVar(&config.BigtableProjectID)
	cmd.Flag("bigtable-instance-id", "The ID of the Cloud Bigtable instance that contains the table").Required().StringVar(&config.BigtableInstanceID)
	cmd.Flag("bigtable-table-id-prefix",
		"Prefix to find the IDs of the Cloud Bigtable table to export. "+
			"It can be a table name to backup specific table or prefix to backup all tables matching the prefix.").Required().StringVar(&config.BigtableTableIDPrefix)
	cmd.Flag("destination-path", "GCS path where data should be written. For example, \"gs://mybucket/somefolder/\"").Required().StringVar(&config.DestinationPath)
	cmd.Flag("temp-prefix", "Path and filename prefix for writing temporary files. ex: gs://MyBucket/tmp").Required().StringVar(&config.TempPrefix)
	cmd.Flag("job-location", "Location where we want to run the job e.g us-central1, europe-west1").Default("us-central1").StringVar(&config.JobLocation)

	return &config
}

// CreateBackup creates the backup.
func CreateBackup(config *CreateBackupConfig) error {
	config.DestinationPath = strings.TrimSuffix(config.DestinationPath, "/")
	unixNow := time.Now().Unix()

	tableIDs, err := listTableIDsWithPrefix(config)
	if err != nil {
		return err
	}

	if len(tableIDs) == 0 {
		return errors.New("No tables found")
	}

	ctx := context.Background()
	service, err := dataflowV1b3.NewService(ctx)
	if err != nil {
		return err
	}

	var jobFailureStates = map[string]struct{}{"JOB_STATE_FAILED": {}, "JOB_STATE_CANCELLED": {}, "JOB_STATE_CANCELLING": {}}

	for _, tableID := range tableIDs {
		jobName := fmt.Sprintf("export-%s-%d", tableID, unixNow)
		destinationPathWithTimestamp := fmt.Sprintf("%s/%s/%d/", config.DestinationPath, tableID, unixNow)
		createJobFromTemplateRequest := dataflowV1b3.CreateJobFromTemplateRequest{
			JobName: jobName,
			GcsPath: bigtableToGCSSequenceFileTemplatePath,
			Parameters: map[string]string{
				"bigtableProject":    config.BigtableProjectID,
				"bigtableInstanceId": config.BigtableInstanceID,
				"bigtableTableId":    tableID,
				"destinationPath":    destinationPathWithTimestamp,
				"filenamePrefix":     tableID + bigtableIDSeparatorInSeqFileName,
			},
			Environment: &dataflowV1b3.RuntimeEnvironment{
				TempLocation: config.TempPrefix,
			},
			Location: config.JobLocation,
		}

		job, err := service.Projects.Templates.Create(config.BigtableProjectID, &createJobFromTemplateRequest).Do()
		if err != nil {
			return fmt.Errorf("Error backing up table with Id %s with error: %s", tableID, err)
		}
		fmt.Printf("Created job for backing up %s with timestamp %d\n", tableID, unixNow)

		// Polling state of the job until its done or fails
		for {
			fetchedJob, err := service.Projects.Locations.Jobs.Get(config.BigtableProjectID, config.JobLocation, job.Id).Do()
			if err != nil {
				return fmt.Errorf("Error getting state of the job with Id %s with error: %s", job.Id, err)
			}

			if state, isOK := jobFailureStates[fetchedJob.CurrentState]; isOK {
				return fmt.Errorf("Data flow job failed with state %s", state)
			}
			if fetchedJob.CurrentState == "JOB_STATE_DONE" {
				break
			}

			fmt.Printf("Current job state: %s\n", fetchedJob.CurrentState)

			time.Sleep(jobStateCheckDuration)
		}
		fmt.Printf("Job for backing up %s with timestamp %d finished\n", tableID, unixNow)
	}

	return nil
}

func listTableIDsWithPrefix(config *CreateBackupConfig) ([]string, error) {
	ctx := context.Background()
	service, err := bigtableAdminV2.NewService(ctx)
	if err != nil {
		return nil, err
	}

	parent := "projects/" + config.BigtableProjectID + "/instances/" + config.BigtableInstanceID
	listTableResponse, err := service.Projects.Instances.Tables.List(parent).Do()
	if err != nil {
		return nil, err
	}

	tableIDs := make([]string, 0, len(listTableResponse.Tables))
	tableID := ""
	for _, table := range listTableResponse.Tables {
		tableID = table.Name[strings.LastIndex(table.Name, "/")+1:]
		if strings.HasPrefix(tableID, config.BigtableTableIDPrefix) {
			tableIDs = append(tableIDs, tableID)
		}
	}

	return tableIDs, nil
}
