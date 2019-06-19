#!/bin/bash

DB="podcatch"

TABLES="
directory_info_cache
file_info_cache
"

for T in $TABLES;
do
    psql $DB -c "DELETE FROM $T";
done
