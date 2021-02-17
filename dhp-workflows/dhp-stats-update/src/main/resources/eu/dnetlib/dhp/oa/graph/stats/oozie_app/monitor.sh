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

echo "Creating monitor database"
cat step21-createMonitorDB.sql | sed s/SOURCE/$1/g | sed s/TARGET/$2/g1 | impala-shell -f -
echo "Impala shell finished"

echo "Updating shadow monitor database"
impala-shell -d $3 -q "show tables" --delimited | sed 's/^/drop view if exists $3./' | sed 's/$/;/' | impala-shell -f -
impala-shell -d $2 -q "show tables" --delimited | sed 's/\(.*\)/create view $3.\1 as select * from $2.\1;/' | impala-shell -f -
echo "Shadow db ready!"
