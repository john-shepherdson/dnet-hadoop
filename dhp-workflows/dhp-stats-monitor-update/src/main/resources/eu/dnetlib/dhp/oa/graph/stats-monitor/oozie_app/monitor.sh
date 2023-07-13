export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export SOURCE=$1
export TARGET=$2
export SHADOW=$3
export SCRIPT_PATH=$4
export SCRIPT_PATH2=$5
export SCRIPT_PATH2=$6

export HIVE_OPTS="-hiveconf mapred.job.queue.name=analytics -hiveconf hive.spark.client.connect.timeout=120000ms -hiveconf hive.spark.client.server.connect.timeout=300000ms -hiveconf spark.executor.memory=19166291558 -hiveconf spark.yarn.executor.memoryOverhead=3225 -hiveconf spark.driver.memory=11596411699 -hiveconf spark.yarn.driver.memoryOverhead=1228"
export HADOOP_USER_NAME="oozie"

echo "Getting file from " $4
hdfs dfs -copyToLocal $4

echo "Getting file from " $5
hdfs dfs -copyToLocal $5

echo "Getting file from " $6
hdfs dfs -copyToLocal $6

#update Institutions DB
cat updateMonitorDB_institutions.sql | sed "s/SOURCE/$1/g" | sed "s/TARGET/$2_institutions/g1" > foo
hive $HIVE_OPTS -f foo
cat updateMonitorDB.sql | sed "s/SOURCE/$1/g" | sed "s/TARGET/$2_institutions/g1" > foo
hive $HIVE_OPTS -f foo

echo "Hive shell finished"

echo "Updating shadow monitor insitutions database"
hive -e "drop database if exists ${SHADOW}_institutions cascade"
hive -e "create database if not exists ${SHADOW}_institutions"
hive $HIVE_OPTS --database ${2}_institutions -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}_institutions.\1 as select * from ${2}_institutions.\1;/" > foo
hive -f foo
echo "Shadow db monitor insitutions ready!"

#update Monitor DB
cat updateMonitorDBAll.sql | sed "s/SOURCE/$1/g" | sed "s/TARGET/$2/g1" > foo
hive $HIVE_OPTS -f foo

echo "Hive shell finished"

echo "Updating shadow monitor database"
hive -e "drop database if exists ${SHADOW} cascade"
hive -e "create database if not exists ${SHADOW}"
hive $HIVE_OPTS --database ${2} -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from ${2}.\1;/" > foo
hive -f foo
echo "Shadow db monitor insitutions ready!"
