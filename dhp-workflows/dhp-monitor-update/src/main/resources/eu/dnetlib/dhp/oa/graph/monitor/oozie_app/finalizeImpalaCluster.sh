export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

function createShadowDB() {
  SOURCE=$1
  SHADOW=$2

  impala-shell -i impala-cluster-dn1.openaire.eu -q "drop database ${SHADOW} CASCADE";
  impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${SHADOW}";
#  impala-shell -i impala-cluster-dn1.openaire.eu -d ${SHADOW} -q "show tables" | sed "s/^/drop view if exists ${SHADOW}./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -f -
  impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -f -
}

STATS_DB=$1
STATS_DB_SHADOW=$2
MONITOR_DB=$3
MONITOR_DB_SHADOW=$4
OBSERVATORY_DB=$5
OBSERVATORY_DB_SHADOW=$6

createShadowDB $STATS_DB $STATS_DB_SHADOW
createShadowDB $MONITOR_DB $MONITOR_DB_SHADOW
createShadowDB $OBSERVATORY_DB $OBSERVATORY_DB_SHADOW
