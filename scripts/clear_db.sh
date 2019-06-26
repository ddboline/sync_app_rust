#!/bin/bash

DB="sync_app_cache"

TABLES="
directory_info_cache
file_info_cache
"

for T in $TABLES;
do
    psql $DB -c "DELETE FROM $T";
done
