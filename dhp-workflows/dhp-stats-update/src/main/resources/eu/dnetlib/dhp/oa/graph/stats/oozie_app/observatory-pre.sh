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

echo "Creating observatory database"
impala-shell -q "drop database if exists ${TARGET} cascade"
impala-shell -q "create database if not exists ${TARGET}"
impala-shell -d ${SOURCE} -q "show tables" --delimited | grep -iv roar | sed "s/\(.*\)/create view ${TARGET}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -f -
