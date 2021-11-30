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

hive --database ${TARGET} -e "show tables" | grep -v WARN | sed "s/\(.*\)/analyze table ${TARGET}.\1 compute statistics;/" > foo
hive -f foo
echo "Impala shell finished"

echo "Updating shadow observatory database"
hive -e "create database if not exists ${SHADOW}"
hive --database ${SHADOW} -e "show tables" | grep -v WARN | sed "s/^/drop view if exists ${SHADOW}./" | sed "s/$/;/" > foo
hive -f foo
hive -d ${TARGET} -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from ${TARGET}.\1;/" > foo
hive -f foo
echo "Shadow db ready!"