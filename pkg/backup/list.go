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

type ListBackupConfig struct {
	BackupPath string
}

func RegisterListBackupsFlags(cmd *kingpin.CmdClause) *ListBackupConfig {
	config := ListBackupConfig{}
	cmd.Flag("backup-path", "GCS path where backups can be found").Required().StringVar(&config.BackupPath)
	return &config
}

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

	numbersOnlyRegex, err := regexp.Compile("^[0-9]*$")
	if err != nil {
		return nil, err
	}
	backupTimestampsMap := make(map[string]map[string]struct{}, len(objects.Items))

	for _, object := range objects.Items {
		ss := strings.SplitN(object.Name[len(objectPrefix):], "/", 3)
		if len(ss) < 3 || ss[2] == ""{
			continue
		}
		tableId := ss[0]
		backupTimestamp := ss[1]
		if !numbersOnlyRegex.Match([]byte(backupTimestamp)) {
			continue
		}

		if _, isOk := backupTimestampsMap[tableId]; !isOk {
			backupTimestampsMap[tableId] = map[string]struct{}{backupTimestamp: {}}
		} else {
			backupTimestampsMap[tableId][backupTimestamp] = struct{}{}
		}
	}

	backupTimestampsList := make(map[string][]string, len(backupTimestampsMap))
	for tableId, backupTimestamps := range backupTimestampsMap {
		backupTimestampsList[tableId] = make([]string, 0, len(backupTimestamps))
		for backupTimestamp := range backupTimestamps {
			backupTimestampsList[tableId] = append(backupTimestampsList[tableId], backupTimestamp)
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

func getNewestBackupTimestamp(backupPath string, tableId string) (*int64, error) {
	backups, err := ListBackups(&ListBackupConfig{backupPath})
	if err != nil {
		return nil, err
	}

	if len(backups) == 0 {
		return nil, errors.New("No backups found")
	}

	backupTimestamps := backups[tableId]
	if len(backupTimestamps) == 0 {
		return nil, errors.New("No backups found")
	}

	newestBackupTimestamp, err := strconv.ParseInt(backupTimestamps[len(backupTimestamps)-1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &newestBackupTimestamp, nil
}
