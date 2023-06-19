export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export SOURCE=$1
export PRODUCTION=$2

echo "Updating ${PRODUCTION} database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION} -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}./" | sed "s/$/;/" | impala-shell -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -c -f -
echo "Production db ready!"