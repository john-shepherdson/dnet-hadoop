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

  # drop views from db
  for i in `impala-shell -i impala-cluster-dn1.openaire.eu -d ${SHADOW} --delimited  -q "show tables"`;
    do
        `impala-shell  -i impala-cluster-dn1.openaire.eu -d -d ${SHADOW} -q "drop view $i;"`;
    done

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
USAGE_STATS_DB=$7
USAGE_STATS_DB_SHADOW=$8

createShadowDB $STATS_DB $STATS_DB_SHADOW
createShadowDB $MONITOR_DB $MONITOR_DB_SHADOW
createShadowDB $OBSERVATORY_DB $OBSERVATORY_DB_SHADOW
createShadowDB USAGE_STATS_DB USAGE_STATS_DB_SHADOW

createShadowDB $MONITOR_DB'_funded' $MONITOR_DB'_funded_shadow'
createShadowDB $MONITOR_DB'_institutions' $MONITOR_DB'_institutions_shadow'
createShadowDB $MONITOR_DB'_RIs_tail' $MONITOR_DB'_RIs_tail_shadow'

contexts="knowmad::other dh-ch::other enermaps::other gotriple::other neanias-atmospheric::other rural-digital-europe::other covid-19::other aurora::other neanias-space::other north-america-studies::other north-american-studies::other eutopia::other"
for i in ${contexts}
do
   tmp=`echo "$i"  | sed 's/'-'/'_'/g' | sed 's/'::'/'_'/g'`
  createShadowDB ${MONITOR_DB}'_'${tmp} ${MONITOR_DB}'_'${tmp}'_shadow'
done