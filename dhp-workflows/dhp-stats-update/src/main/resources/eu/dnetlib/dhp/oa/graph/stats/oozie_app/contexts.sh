export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

CONTEXT_API=$1
TARGET_DB=$2

export HIVE_OPTS="-hiveconf mapred.job.queue.name=analytics -hiveconf hive.spark.client.connect.timeout=120000ms -hiveconf hive.spark.client.server.connect.timeout=300000ms -hiveconf spark.executor.memory=4831838208 -hiveconf spark.yarn.executor.memoryOverhead=450"

TMP=/tmp/stats-update-`tr -dc A-Za-z0-9 </dev/urandom | head -c 6`

echo "Downloading context ids"
curl -L ${CONTEXT_API}/contexts/?type=ri,community -H "accept: application/json" | /usr/local/sbin/jq -r '.[] | "\(.id),\(.label)"' > contexts.csv

echo "Downloading categories data"
cat contexts.csv | cut -d , -f1 | xargs -I {} curl -L ${CONTEXT_API}/context/{}/?all=true | /usr/local/sbin/jq -r '.[]|"\(.id|split(":")[0]),\(.id),\(.label)"' > categories.csv

echo "Downloading concepts data"
cat categories.csv | cut -d , -f2 | sed 's/:/%3A/g'| xargs -I {} curl -L ${CONTEXT_API}/context/category/{}/?all=true | /usr/local/sbin/jq -r '.[]|"\(.id|split("::")[0])::\(.id|split("::")[1]),\(.id),\(.label)"' > concepts.csv
cat contexts.csv | sed 's/^\(.*\),\(.*\)/\1,\1::other,\2/' >> categories.csv
cat categories.csv | sed 's/^.*,\(.*\),\(.*\)/\1,\1::other,\2/' >> concepts.csv

echo "uploading context data to hdfs"
hdfs dfs -mkdir ${TMP}
hdfs dfs -copyFromLocal contexts.csv ${TMP}
hdfs dfs -copyFromLocal categories.csv ${TMP}
hdfs dfs -copyFromLocal concepts.csv ${TMP}
hdfs dfs -chmod -R 777 ${TMP}

export HADOOP_USER="oozie"
export HADOOP_USER_NAME="oozie"

echo "Creating and populating impala tables"
hive $HIVE_OPTS -e "create table ${TARGET_DB}.context_csv (id string, name string) row format delimited fields terminated by ','"
hive $HIVE_OPTS -e "load data inpath '${TMP}/contexts.csv' into table ${TARGET_DB}.context_csv"
hive $HIVE_OPTS -e "create table ${TARGET_DB}.context stored as parquet as select * from ${TARGET_DB}.context_csv"
hive $HIVE_OPTS -e "drop table ${TARGET_DB}.context_csv purge"

hive $HIVE_OPTS -e "create table ${TARGET_DB}.category_csv (context string, id string, name string) row format delimited fields terminated by ','"
hive $HIVE_OPTS -e "load data inpath '${TMP}/categories.csv' into table ${TARGET_DB}.category_csv"
hive $HIVE_OPTS -e "create table ${TARGET_DB}.category stored as parquet as select * from ${TARGET_DB}.category_csv"
hive $HIVE_OPTS -e "drop table ${TARGET_DB}.category_csv purge"

hive $HIVE_OPTS -e "create table ${TARGET_DB}.concept_csv (category string, id string, name string) row format delimited fields terminated by ','"
hive $HIVE_OPTS -e "load data inpath '${TMP}/concepts.csv' into table ${TARGET_DB}.concept_csv"
hive $HIVE_OPTS -e "create table ${TARGET_DB}.concept stored as parquet as select * from ${TARGET_DB}.concept_csv"
hive $HIVE_OPTS -e "drop table ${TARGET_DB}.concept_csv purge"

echo "Cleaning up"
rm concepts.csv
rm categories.csv
rm contexts.csv

echo "Finito!"
