export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export TARGET=$1
export SCRIPT_PATH=$2

echo "Getting file from " $SCRIPT_PATH
hdfs dfs -copyToLocal $SCRIPT_PATH

echo "Creating indicators"
hive --database ${TARGET} -e "show tables" | grep -v WARN | sed "s/^\(.*\)/analyze table ${TARGET}.\1 compute statistics;/" > foo
hive -f foo
hive --database ${TARGET} -f step16-createIndicatorsTables.sql
echo "Indicators created"