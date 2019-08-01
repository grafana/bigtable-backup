package backup

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"

	storageV1 "google.golang.org/api/storage/v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

// ListBackupConfig has the config for ListBackup command.
type ListBackupConfig struct {
	BackupPath   string
	OutputFormat string
}

// RegisterListBackupsFlags registers the flags for list backups.
func RegisterListBackupsFlags(cmd *kingpin.CmdClause) *ListBackupConfig {
	config := ListBackupConfig{}
	cmd.Flag("backup-path", "GCS path where backups can be found").Required().StringVar(&config.BackupPath)
	cmd.Flag("output", "Output Format. Support json, text. Defaults to text").Short('o').StringVar(&config.OutputFormat)
	return &config
}

// ListBackups lists the available backups. It returns a map from the tableID
// to the backup timestamps for that table.
func ListBackups(config *ListBackupConfig) (map[string][]string, error) {
	ctx := context.Background()
	service, err := storageV1.NewService(ctx)
	if err != nil {
		return nil, err
	}

	bucketName, objectPrefix := getBucketNameAndObjectPrefix(config.BackupPath)

	objectListCall := service.Objects.List(bucketName)
	if objectPrefix != "" {
		objectListCall.Prefix(objectPrefix)
	}

	objects, err := objectListCall.Do()
	if err != nil {
		return nil, err
	}

	numbersOnlyRegex := regexp.MustCompile("^[0-9]*$")

	// tableID --> timestamp.
	backupTimestampsMap := make(map[string]map[string]struct{}, len(objects.Items))

	for _, object := range objects.Items {
		ss := strings.SplitN(object.Name[len(objectPrefix):], "/", 3)
		if len(ss) < 3 || ss[2] == "" {
			continue
		}
		tableID := ss[0]
		backupTimestamp := ss[1]
		if !numbersOnlyRegex.Match([]byte(backupTimestamp)) {
			continue
		}

		if _, isOk := backupTimestampsMap[tableID]; !isOk {
			backupTimestampsMap[tableID] = map[string]struct{}{backupTimestamp: {}}
		} else {
			backupTimestampsMap[tableID][backupTimestamp] = struct{}{}
		}
	}

	backupTimestampsList := make(map[string][]string, len(backupTimestampsMap))
	for tableID, backupTimestamps := range backupTimestampsMap {
		backupTimestampsList[tableID] = make([]string, 0, len(backupTimestamps))
		for backupTimestamp := range backupTimestamps {
			backupTimestampsList[tableID] = append(backupTimestampsList[tableID], backupTimestamp)
		}
	}

	return backupTimestampsList, nil
}

func getBucketNameAndObjectPrefix(backupPath string) (bucketName, objectPrefix string) {
	if strings.HasPrefix(backupPath, "gs://") {
		backupPath = backupPath[5:]
	}
	ss := strings.SplitN(backupPath, "/", 2)
	bucketName = ss[0]

	if len(ss) == 2 {
		objectPrefix = ss[1]
		if !strings.HasSuffix(objectPrefix, "/") {
			objectPrefix = objectPrefix + "/"
		}
	}

	return
}

func getNewestBackupTimestamp(backupPath string, tableID string) (*int64, error) {
	backups, err := ListBackups(&ListBackupConfig{BackupPath: backupPath})
	if err != nil {
		return nil, err
	}

	if len(backups) == 0 {
		return nil, errors.New("No backups found")
	}

	backupTimestamps := backups[tableID]
	if len(backupTimestamps) == 0 {
		return nil, errors.New("No backups found")
	}

	// TODO(goutham): This is lexicographic sorting. Will break in a "few" years.
	newestBackupTimestamp, err := strconv.ParseInt(backupTimestamps[len(backupTimestamps)-1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &newestBackupTimestamp, nil
}
