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

export HIVE_OPTS="-hiveconf mapred.job.queue.name=analytics -hiveconf hive.spark.client.connect.timeout=120000ms -hiveconf hive.spark.client.server.connect.timeout=300000ms -hiveconf spark.executor.memory=19166291558 -hiveconf spark.yarn.executor.memoryOverhead=3225 -hiveconf spark.driver.memory=11596411699 -hiveconf spark.yarn.driver.memoryOverhead=1228"
export HADOOP_USER_NAME="oozie"

echo "Getting file from " $SCRIPT_PATH
hdfs dfs -copyToLocal $SCRIPT_PATH

echo "Creating monitor database"
#cat step20-createMonitorDB.sql | sed s/SOURCE/$1/g | sed s/TARGET/$2/g1 > foo
cat step20-createMonitorDB.sql | sed "s/TARGET/${TARGET}/g" | sed "s/SOURCE/${SOURCE}/g1" > foo
hive $HIVE_OPTS -f foo
echo "Hive shell finished"