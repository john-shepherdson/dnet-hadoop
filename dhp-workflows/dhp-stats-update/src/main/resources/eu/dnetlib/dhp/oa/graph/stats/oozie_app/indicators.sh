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
impala-shell -q "invalidate metadata"
impala-shell -d ${TARGET} -q "show tables" --delimited | sed "s/^\(.*\)/compute stats ${TARGET}.\1;/" | impala-shell -c -f -
cat step16_7-createIndicatorsTables.sql | impala-shell -d $TARGET -f -
echo "Indicators created"