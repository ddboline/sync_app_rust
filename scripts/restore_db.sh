#!/bin/bash

DB="podcatch"
BUCKET="podcatch-db-backup"

TABLES="
directory_info_cache
file_info_cache
"

mkdir -p backup/

for T in $TABLES;
do
    aws s3 cp s3://${BACKUP}/${T}.sql.gz backup/${T}.sql.gz
    gzip -dc backup/${T}.sql.gz | psql $DB -c "COPY $T FROM STDIN"
done
