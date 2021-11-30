export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export SOURCE=$1
export SHADOW=$2

echo "Updating shadow database"
hive --database ${SOURCE} -e "show tables" | grep -v WARN | sed "s/^\(.*\)/analyze table ${SOURCE}.\1 compute statistics;/" > foo
hive -f foo
hive -e "create database if not exists ${SHADOW}"
hive --database ${SHADOW} -e "show tables" | grep -v WARN | sed "s/^/drop view if exists ${SHADOW}./" | sed "s/$/;/" > foo
hive -f foo
hive --database ${SOURCE} -e "show tables" | grep -v WARN | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from ${SOURCE}.\1;/" > foo
hive -f foo
echo "Shadow db ready!"