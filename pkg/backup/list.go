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

func ListBackups(config *ListBackupConfig) ([]string, error) {
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
	backupTimestampsMap := make(map[string]struct{}, len(objects.Items))

	for _, object := range objects.Items {
		backupTimestamp := strings.SplitN(object.Name[len(objectPrefix):], "/", 2)[0]

		if _, isOK := backupTimestampsMap[backupTimestamp]; isOK {
			continue
		}

		if numbersOnlyRegex.Match([]byte(backupTimestamp)) {
			backupTimestampsMap[backupTimestamp] = struct{}{}
		}
	}

	backupTimestamps := make([]string, 0, len(backupTimestampsMap))
	for backupTimestamp := range backupTimestampsMap {
		backupTimestamps = append(backupTimestamps, backupTimestamp)
	}

	return backupTimestamps, nil
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

func getNewestBackupTimestamp(backupPath string) (*int64, error) {
	backupTimestamps, err := ListBackups(&ListBackupConfig{backupPath})
	if err != nil {
		return nil, err
	}

	if len(backupTimestamps) == 0 {
		return nil, errors.New("No backups found")
	}

	newestBackupTimestamp, err := strconv.ParseInt(backupTimestamps[len(backupTimestamps)-1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &newestBackupTimestamp, nil
}
