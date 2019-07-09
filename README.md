# bigtable-backup
Helps with creating and restoring Bigtable backups. Backups are exported to and imported from GCS in Sequence file format. Links can be found down below to read more on it.

## Getting started

```
$ go get -u github.com/sandlis/bigtable-backup
```

### Usage:
```
$ bigtable-backup --help
usage: bigtable-backup [<flags>] <command> [<args> ...]

A command-line for creating and restoring backups from bigtable.

Flags:
  --help  Show context-sensitive help (also try --help-long and --help-man).

Commands:
  help [<command>...]
    Show help.

  create --bigtable-project-id=BIGTABLE-PROJECT-ID --bigtable-instance-id=BIGTABLE-INSTANCE-ID --bigtable-table-id-prefix=BIGTABLE-TABLE-ID-PREFIX --destination-path=DESTINATION-PATH --temp-prefix=TEMP-PREFIX [<flags>]
    Create backups for specific table or active periodic table or all the tables for given prefix

  list-backups --backup-path=BACKUP-PATH
    Restore backups of all or specific bigtableTableId created for specific timestamp

  restore --backup-path=BACKUP-PATH --bigtable-project-id=BIGTABLE-PROJECT-ID --bigtable-instance-id=BIGTABLE-INSTANCE-ID --bigtable-table-id=BIGTABLE-TABLE-ID --temp-prefix=TEMP-PREFIX [<flags>]
    Restore backups of specific bigtableTableId created at a timestamp

  delete-backup --bigtable-table-id=BIGTABLE-TABLE-ID --backup-path=BACKUP-PATH --backup-timestamp=BACKUP-TIMESTAMP
    Delete backup of a table with timestamp
```

### Note:
- While restoring from a backup, table should already exist in Bigtable.

### Authentication:
Using a service account is recommended here with permission to read and write to Dataflow, GCS and Bigtable.
More information on Authentication can be found [here](https://cloud.google.com/docs/authentication/getting-started)

## Further Reading about Google Cloud Dataflow, which is used for creating and restoring backups
[Cloud Bigtable to Cloud Storage SequenceFile](https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#cloudbigtabletosequencefile)

[Cloud Storage SequenceFile to Cloud Bigtable](https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#sequencefiletocloudbigtable)
