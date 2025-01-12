export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi
#
#function createShadowDB() {
#  SOURCE=$1
#  SHADOW=$2
#
#  # drop views from db
#  for i in `impala-shell -i impala-cluster-dn1.openaire.eu -d ${SHADOW} --delimited  -q "show tables"`;
#    do
#        `impala-shell  -i impala-cluster-dn1.openaire.eu -d ${SHADOW} -q "drop view $i;"`;
#    done
#
#  impala-shell -i impala-cluster-dn1.openaire.eu -q "drop database ${SHADOW} CASCADE";
#  impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${SHADOW}";
##  impala-shell -i impala-cluster-dn1.openaire.eu -d ${SHADOW} -q "show tables" | sed "s/^/drop view if exists ${SHADOW}./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -f -
#  impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${SHADOW}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -f -
#}
#
#MONITOR_DB=$1
#MONITOR_DB_SHADOW=$2
#
#createShadowDB $MONITOR_DB'_institutions' $MONITOR_DB'_institutions_shadow'
#createShadowDB $MONITOR_DB $MONITOR_DB'_shadow'

SOURCE=$1
PRODUCTION=$2
echo ${SOURCE}
echo ${PRODUCTION}

#echo "Updating ${PRODUCTION} monitor database old cluster"
#impala-shell -q "create database if not exists ${PRODUCTION}"
#impala-shell -d ${PRODUCTION} -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}./" | sed "s/$/;/" | impala-shell -c -f -
#impala-shell -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -c -f -
#
#echo "Updating ${PRODUCTION}_institutions database old cluster"
#impala-shell -q "create database if not exists ${PRODUCTION}_institutions"
#impala-shell -d ${PRODUCTION}_institutions -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}_institutions./" | sed "s/$/;/" | impala-shell -c -f -
#impala-shell -d ${SOURCE}_institutions -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}_institutions.\1 as select * from ${SOURCE}_institutions.\1;/" | impala-shell -c -f -
#echo "Production insitutions db ready!"

echo "Updating ${PRODUCTION} monitor database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION} -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
echo "Production monitor db ready!"

echo "Updating ${PRODUCTION}_institutions database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}_institutions"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION}_institutions -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}_institutions./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE}_institutions -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}_institutions.\1 as select * from ${SOURCE}_institutions.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
echo "Production insitutions db ready!"