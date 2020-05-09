#!/bin/bash

DB="sync_app_cache"
BUCKET="sync-app-cache-db-backup"

TABLES="
directory_info_cache
file_info_cache
"

mkdir -p backup/

for T in $TABLES;
do
    aws s3 cp s3://${BUCKET}/${T}.sql.gz backup/${T}.sql.gz
    gzip -dc backup/${T}.sql.gz | psql $DB -c "COPY $T FROM STDIN"
done

psql $DB -c "select setval('file_info_cache_id_seq', (select max(id) from file_info_cache), TRUE)"
