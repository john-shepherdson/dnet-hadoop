export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

export SOURCE=$1
export PRODUCTION=$2

echo "Updating ${PRODUCTION} monitor database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION} -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE} -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}.\1 as select * from ${SOURCE}.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
echo "Production monitor db ready!"

echo "Updating ${PRODUCTION}_funded database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}_funded"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION}_funded -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}_funded./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE}_funded -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}_funded.\1 as select * from ${SOURCE}_funded.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
echo "Production funded db ready!"

echo "Updating ${PRODUCTION}_institutions database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}_institutions"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION}_institutions -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}_institutions./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE}_institutions -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}_institutions.\1 as select * from ${SOURCE}_institutions.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
echo "Production insitutions db ready!"

echo "Updating ${PRODUCTION}_ris_tail database"
impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}_ris_tail"
impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION}_ris_tail -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}_ris_tail./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE}_ris_tail -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}_ris_tail.\1 as select * from ${SOURCE}_ris_tail.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
echo "Production RIS tail db ready!"

contexts="knowmad::other dh-ch::other enermaps::other gotriple::other neanias-atmospheric::other rural-digital-europe::other covid-19::other aurora::other neanias-space::other north-america-studies::other north-american-studies::other eutopia::other"
for i in ${contexts}
do
   tmp=`echo "$i"  | sed 's/'-'/'_'/g' | sed 's/'::'/'_'/g'`
  impala-shell -i impala-cluster-dn1.openaire.eu -q "create database if not exists ${PRODUCTION}_${tmp}"
  impala-shell -i impala-cluster-dn1.openaire.eu -d ${PRODUCTION}_${tmp} -q "show tables" --delimited | sed "s/^/drop view if exists ${PRODUCTION}_${tmp}./" | sed "s/$/;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
  impala-shell -i impala-cluster-dn1.openaire.eu -d ${SOURCE}_${tmp} -q "show tables" --delimited | sed "s/\(.*\)/create view ${PRODUCTION}_${tmp}.\1 as select * from ${SOURCE}_${tmp}.\1;/" | impala-shell -i impala-cluster-dn1.openaire.eu -c -f -
  echo "Production ${tmp} db ready!"
done
