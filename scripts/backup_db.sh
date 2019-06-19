#!/bin/bash

DB="sync_app_cache"
BUCKET="sync-app-cache-db-backup"

TABLES="
directory_info_cache
file_info_cache
"

mkdir -p backup

for T in $TABLES;
do
    psql $DB -c "COPY $T TO STDOUT" | gzip > backup/${T}.sql.gz
    aws s3 cp backup/${T}.sql.gz s3://${BUCKET}/${T}.sql.gz
done
