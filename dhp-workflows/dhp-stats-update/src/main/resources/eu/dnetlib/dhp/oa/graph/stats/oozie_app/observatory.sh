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

echo "Getting file from " $4
hdfs dfs -copyToLocal $4

echo "Creating observatory database"
impala-shell -q "drop database if exists ${TARGET} cascade"
impala-shell -q "create database if not exists ${TARGET}"
impala-shell -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${TARGET}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -f -
cat step21-createObservatoryDB.sql | sed s/SOURCE/$1/g | sed s/TARGET/$2/g1 | hive -f -
impala-shell -q "invalidate metadata;"
impala-shell -d ${TARGET} -q "show tables" --delimited | sed "s/\(.*\)/compute stats ${TARGET}.\1;/" | impala-shell -f -
echo "Impala shell finished"

echo "Updating shadow observatory database"
impala-shell -q "create database if not exists ${SHADOW}"
impala-shell -d ${SHADOW} -q "show tables" --delimited | sed "s/^/drop view if exists ${SHADOW}./" | sed "s/$/;/" | impala-shell -f -
impala-shell -d ${TARGET} -q "show tables" --delimited | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from ${TARGET}.\1;/" | impala-shell -f -
echo "Shadow db ready!"