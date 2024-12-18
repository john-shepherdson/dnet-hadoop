export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

SOURCE=$1
PRODUCTION=$2
SHADOW=$3
MONITOR_PROD=$4
MONITOR_IRISH_PROD=$5


echo ${SOURCE}
echo ${PRODUCTION}

#echo "Updating ${PRODUCTION} monitor database old cluster"
#impala-shell -q "create database if not exists ${PRODUCTION}"
#impala-shell -d ${PRODUCTION} -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}./" | sed "s/$/;/" | impala-shell -c -f -
#impala-shell -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -c -f -

echo "Updating ${PRODUCTION} historical snapshots database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION} -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
echo "Production monitor db ready!"

impala-shell -i impala-cluster-dn1.openaire.eu -q "drop view ${MONITOR_PROD}.historical_snapshots"
impala-shell -i impala-cluster-dn1.openaire.eu -q "drop view ${MONITOR_PROD}.historical_snapshots_fos"

impala-shell -i impala-cluster-dn1.openaire.eu -q "create view ${MONITOR_PROD}.historical_snapshots as select * from ${SOURCE}.historical_snapshots"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create view ${MONITOR_PROD}.historical_snapshots_fos as select * from ${SOURCE}.historical_snapshots_fos"

impala-shell -i impala-cluster-dn1.openaire.eu -q "drop view ${MONITOR_IRISH_PROD}.historical_snapshots_irish"
impala-shell -i impala-cluster-dn1.openaire.eu -q "drop view ${MONITOR_IRISH_PROD}.historical_snapshots_irish_fos"


impala-shell -i impala-cluster-dn1.openaire.eu -q "create view ${MONITOR_IRISH_PROD}.historical_snapshots_irish as select * from ${SOURCE}.historical_snapshots_irish"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create view ${MONITOR_IRISH_PROD}.historical_snapshots_irish_fos as select * from ${SOURCE}.historical_snapshots_irish"
