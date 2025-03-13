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

export HIVE_OPTS="-hiveconf mapred.job.queue.name=analytics -hiveconf hive.spark.client.connect.timeout=120000ms -hiveconf hive.spark.client.server.connect.timeout=300000ms -hiveconf spark.executor.memory=19166291558 -hiveconf spark.yarn.executor.memoryOverhead=3225 -hiveconf spark.driver.memory=11596411699 -hiveconf spark.yarn.driver.memoryOverhead=1228"
export HADOOP_USER_NAME="oozie"

echo "Getting file from " "$4"
hdfs dfs -copyToLocal "$4"

echo "Getting file from " "$5"
hdfs dfs -copyToLocal "$5"

echo "Getting file from " "$6"
hdfs dfs -copyToLocal "$6"

echo "Getting file from " "$7"
hdfs dfs -copyToLocal "$7"

echo "Getting file from " "$8"
hdfs dfs -copyToLocal "$8"

echo "Getting file from " "$9"
hdfs dfs -copyToLocal "$9"

echo "Getting file from " "${10}"
hdfs dfs -copyToLocal "${10}"


echo "Creating monitor database(s)"
cat step20-createMonitorDBAll.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET/g1" > foo
hive $HIVE_OPTS -f foo

cat step20-createMonitorDB_funded.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_funded/g1" > foo
hive $HIVE_OPTS -f foo
cat step20-createMonitorDB.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_funded/g1" > foo
hive $HIVE_OPTS -f foo

cat step20-createMonitorDB_institutions.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_institutions/g1" > foo
hive $HIVE_OPTS -f foo
cat step20-createMonitorDB.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_institutions/g1" > foo
hive $HIVE_OPTS -f foo

contexts="knowmad::other dh-ch::other enermaps::other gotriple::other neanias-atmospheric::other rural-digital-europe::other covid-19::other aurora::other neanias-space::other north-america-studies::other north-american-studies::other eutopia::other"

for i in ${contexts}
do
  tmp=`echo "$i"  | sed 's/'-'/'_'/g' | sed 's/'::'/'_'/g'`
  tmp2=`echo "$i" |sed 's/:.*//' `
  cat step20-createMonitorDB_RIs.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_$tmp/g1" | sed "s/CONTEXT/\'%$tmp2%\'/g" > foo
  hive $HIVE_OPTS -f foo
  cat step20-createMonitorDB.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_$tmp/g1" > foo
  hive $HIVE_OPTS -f foo
done

cat step20-createMonitorDB_RIs_tail.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_ris_tail/g1" | sed "s/CONTEXTS/\"'knowmad::other','dh-ch::other', 'enermaps::other', 'gotriple::other', 'neanias-atmospheric::other', 'rural-digital-europe::other', 'covid-19::other', 'aurora::other', 'neanias-space::other', 'north-america-studies::other', 'north-american-studies::other', 'eutopia::other'\"/g" > foo
hive $HIVE_OPTS -f foo
cat step20-createMonitorDB.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_ris_tail/g1" > foo
hive $HIVE_OPTS -f foo


cat step20-createIrishDB.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_monitor_ie/g1" > foo
hive $HIVE_OPTS -f foo
cat step20-createMonitorDB.sql | sed "s/SOURCE/$SOURCE/g" | sed "s/TARGET/$TARGET_monitor_ie/g1" > foo
hive $HIVE_OPTS -f foo

echo "Hive shell finished"

echo "Updating shadow monitor all database"
hive -e "drop database if exists ${SHADOW} cascade"
hive -e "create database if not exists ${SHADOW}"
hive $HIVE_OPTS --database $TARGET -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from $TARGET.\1;/" > foo
hive -f foo
echo "Updated shadow monitor all database"

echo "Updating shadow monitor funded database"
hive -e "drop database if exists ${SHADOW}_funded cascade"
hive -e "create database if not exists ${SHADOW}_funded"
hive $HIVE_OPTS --database $TARGET_funded -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}_funded.\1 as select * from $TARGET_funded.\1;/" > foo
hive -f foo
echo "Updated shadow monitor funded database"

echo "Updating shadow monitor institutions database"
hive -e "drop database if exists ${SHADOW}_institutions cascade"
hive -e "create database if not exists ${SHADOW}_institutions"
hive $HIVE_OPTS --database $TARGET_institutions -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}_institutions.\1 as select * from $TARGET_institutions.\1;/" > foo
hive -f foo
echo "Shadow db monitor institutions ready!"

echo "Updating shadow monitor RIs database"
for i in $contexts
do
  tmp=`echo "$i"  | sed 's/'-'/'_'/g' | sed 's/'::'/'_'/g'`
  hive -e "drop database if exists ${SHADOW}_${tmp} cascade"
  hive -e "create database if not exists ${SHADOW}_${tmp}"
  hive $HIVE_OPTS --database $TARGET_${tmp} -e "show tables" | grep -v WARN  | sed "s/\(.*\)/create view ${SHADOW}_${tmp}.\1 as select * from $TARGET_${tmp}.\1;/" > foo
  hive -f foo
done
echo "Shadow db monitor RIs ready!"

echo "Updating shadow monitor RIs tail database"
hive -e "drop database if exists ${SHADOW}_ris_tail cascade"
hive -e "create database if not exists ${SHADOW}_ris_tail"
hive $HIVE_OPTS --database $TARGET_ris_tail -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}_ris_tail.\1 as select * from $TARGET_ris_tail.\1;/" > foo
hive -f foo
echo "Shadow db monitor RIs tail ready!"

echo "Updating shadow irish monitor database"
hive -e "drop database if exists ${SHADOW}_monitor_ie cascade"
hive -e "create database if not exists ${SHADOW}_monitor_ie"
hive $HIVE_OPTS --database $TARGET_monitor_ie -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}_monitor_ie.\1 as select * from $TARGET_monitor_ie.\1;/" > foo
hive -f foo
echo "Shadow db irish monitor ready!"

























