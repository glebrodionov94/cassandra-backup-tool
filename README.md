# cassandra-backup-tool
Backup and restore utility for Apache Cassandra written in Python.

python cassandra_backup.py backup myks ./backups/myks \
  --hosts 10.0.0.1,10.0.0.2 \
  --username cassandra --password secret \
  --read-consistency LOCAL_QUORUM

python cassandra_backup.py restore myks ./backups/myks \
  --hosts 10.0.0.1 \
  --username cassandra --password secret \
  --write-consistency ALL
