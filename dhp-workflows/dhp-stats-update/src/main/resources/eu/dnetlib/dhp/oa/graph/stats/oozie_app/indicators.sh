export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export TARGET=$1
export STATS_EXT=$2
export SCRIPT_PATH=$3

export HIVE_OPTS="-hiveconf mapred.job.queue.name=analytics -hiveconf hive.spark.client.connect.timeout=120000ms -hiveconf hive.spark.client.server.connect.timeout=300000ms -hiveconf spark.executor.memory=19166291558 -hiveconf spark.yarn.executor.memoryOverhead=3225 -hiveconf spark.driver.memory=11596411699 -hiveconf spark.yarn.driver.memoryOverhead=1228 -hiveconf hive.auto.convert.join=false"
export HADOOP_USER_NAME="oozie"

echo "Getting file from " $SCRIPT_PATH
hdfs dfs -copyToLocal $SCRIPT_PATH

echo "Creating indicators"
hive $HIVE_OPTS --database ${TARGET} -e "show tables" | grep -v WARN | sed "s/STATS_EXT/${STATS_EXT}/g"  |sed "s/^\(.*\)/analyze table ${TARGET}.\1 compute statistics;/" > foo
hive $HIVE_OPTS -f foo
hive $HIVE_OPTS --database ${TARGET} -f step16-createIndicatorsTables.sql
echo "Indicators created"
