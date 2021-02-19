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
impala-shell -q "invalidate metadata ${SOURCE}"
impala-shell -d ${SOURCE} -q "show tables" --delimited | sed "s/^\(.*\)/compute stats ${SOURCE}.\1;/" | impala-shell -c -f -
impala-shell -q "create database if not exists ${SHADOW}"
impala-shell -d ${SHADOW} -q "show tables" --delimited | sed "s/^/drop view if exists ${SHADOW}./" | sed "s/$/;/" | impala-shell -f -
impala-shell -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -f -
echo "Shadow db ready!"