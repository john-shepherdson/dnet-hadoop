export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export SOURCE=$1
export TARGET=$2
export SCRIPT_PATH=$3

echo "Getting file from " $3
hdfs dfs -copyToLocal $3

echo "Creating monitor database"
cat createMonitorDB.sql | sed s/SOURCE/$1/g | sed s/TARGET/$2/g1 | impala-shell -f -
echo "Impala shell finished"
