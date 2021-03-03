#!/usr/bin/env bash

CONTEXT_API=$1
TARGET_DB=$2

TMP=/tmp/stats-update-`tr -dc A-Za-z0-9 </dev/urandom | head -c 6`

echo "Downloading context data"
curl ${CONTEXT_API}/contexts?all=true -H "accept: application/json" | /usr/local/sbin/jq -r '.[] | "\(.id),\(.label)"' > contexts.csv
cat contexts.csv | cut -d , -f1 | xargs -I {} curl ${CONTEXT_API}/context/{}/?all=true | /usr/local/sbin/jq -r '.[]|"\(.id|split(":")[0]),\(.id),\(.label)"' > categories.csv
cat categories.csv | cut -d , -f2 | sed 's/:/%3A/g'| xargs -I {} curl ${CONTEXT_API}/context/category/{}/?all=true | /usr/local/sbin/jq -r '.[]|"\(.id|split("::")[0])::\(.id|split("::")[1]),\(.id),\(.label)"' > concepts.csv
cat contexts.csv  | cut -f1 -d, | sed 's/\(.*\)/\1,\1::other,other/' >> categories.csv
cat categories.csv | cut -d, -f2 | sed 's/\(.*\)/\1,\1::other,other/' >> concepts.csv

echo "uploading context data to hdfs"
hdfs dfs -mkdir ${TMP}
hdfs dfs -copyFromLocal contexts.csv ${TMP}
hdfs dfs -copyFromLocal categories.csv ${TMP}
hdfs dfs -copyFromLocal concepts.csv ${TMP}
hdfs dfs -chmod -R 777 ${TMP}

echo "Creating and populating impala tables"
impala-shell -c "create table ${TARGET_DB}.context (id string, name string) row format delimited fields terminated by ',';"
impala-shell -c "create table ${TARGET_DB}.category (context string, id string, name string) row format delimited fields terminated by ',';"
impala-shell -c "create table ${TARGET_DB}.concept (category string, id string, name string) row format delimited fields terminated by ',';"
impala-shell -c "load data inpath '${TMP}/contexts.csv' into table ${TARGET_DB}.context;"
impala-shell -c "load data inpath '${TMP}/categories.csv' into table ${TARGET_DB}.category;"
impala-shell -c "load data inpath '${TMP}/concepts.csv' into table ${TARGET_DB}.concept;"

echo "Cleaning up"
hdfs dfs -rm -f -r -skipTrash ${TMP}

echo "Finito!"