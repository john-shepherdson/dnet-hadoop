export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

#export HADOOP_USER_NAME="dimitris.pierrakos"
export HADOOP_USER_NAME=$6
export PROD_USAGE_STATS_DB="openaire_prod_usage_stats"
function copydb() {
  db=$1
  FILE=("hive_wf_tmp_"$RANDOM)
  hdfs dfs -mkdir hdfs://impala-cluster-mn1.openaire.eu:8020/tmp/$FILE/
  # copy the databases from ocean to impala

  echo "copying $db"
  hadoop distcp -Dmapreduce.map.memory.mb=6144 -pb hdfs://nameservice1/user/hive/warehouse/${db}.db hdfs://impala-cluster-mn1.openaire.eu:8020/tmp/$FILE/

  # change ownership to impala
  hdfs dfs -conf /etc/impala_cluster/hdfs-site.xml -chmod -R 777 /tmp/$FILE/${db}.db

  # drop tables from db
  for i in `impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -d ${db} --delimited  -q "show tables"`;
    do
        `impala-shell  -i impala-cluster-dn1.openaire.eu -d -d ${db} -q "drop table $i;"`;
    done

  # drop views from db
  for i in `impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -d ${db} --delimited  -q "show tables"`;
    do
        `impala-shell  -i impala-cluster-dn1.openaire.eu -d -d ${db} -q "drop view $i;"`;
    done

  # delete the database
  impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -q "drop database if exists ${db} cascade";

  # create the databases
  impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -q "create database ${db}";

  impala-shell --user $HADOOP_USER_NAME -q "INVALIDATE METADATA"
  echo "creating schema for ${db}"
  for ((  k  = 0;  k  < 5;  k ++ )); do
  for i in `impala-shell --user $HADOOP_USER_NAME -d ${db} --delimited  -q "show tables"`;
    do
      impala-shell --user $HADOOP_USER_NAME -d ${db} --delimited  -q "show create table $i";
    done |  sed 's/"$/;/' | sed 's/^"//' | sed 's/[[:space:]]\date[[:space:]]/`date`/g' | impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -c -f -
  done

#  for i in `impala-shell --user $HADOOP_USER_NAME -d ${db} --delimited  -q "show tables"`;
#    do
#      impala-shell --user $HADOOP_USER_NAME -d ${db} --delimited  -q "show create table $i";
#    done |  sed 's/"$/;/' | sed 's/^"//' | sed 's/[[:space:]]\date[[:space:]]/`date`/g' | impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -c -f -
#
#  # run the same command twice because we may have failures in the first run (due to views pointing to the same db)
#  for i in `impala-shell --user $HADOOP_USER_NAME -d ${db} --delimited  -q "show tables"`;
#    do
#      impala-shell --user $HADOOP_USER_NAME -d ${db} --delimited  -q "show create table $i";
#    done |  sed 's/"$/;/' | sed 's/^"//' | sed 's/[[:space:]]\date[[:space:]]/`date`/g' | impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -c -f -

  # load the data from /tmp in the respective tables
  echo "copying data in tables and computing stats"
  for i in `impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -d ${db} --delimited  -q "show tables"`;
      do
        impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -d ${db} -q "load data inpath '/tmp/$FILE/${db}.db/$i' into table $i";
        impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -d ${db} -q "compute stats $i";
      done

  # deleting the remaining directory from hdfs
hdfs dfs -conf /etc/impala_cluster/hdfs-site.xml -rm -R /tmp/$FILE/${db}.db
}

STATS_DB=$1
MONITOR_DB=$2
OBSERVATORY_DB=$3
EXT_DB=$4
USAGE_STATS_DB=$5
HADOOP_USER_NAME=$6

copydb $USAGE_STATS_DB
copydb $PROD_USAGE_STATS_DB
copydb $EXT_DB
copydb $STATS_DB
#copydb $MONITOR_DB
copydb $OBSERVATORY_DB

copydb $MONITOR_DB'_funded'
copydb $MONITOR_DB'_institutions'
copydb $MONITOR_DB'_RIs_tail'

contexts="knowmad::other dh-ch::other enermaps::other gotriple::other neanias-atmospheric::other rural-digital-europe::other covid-19::other aurora::other neanias-space::other north-america-studies::other north-american-studies::other eutopia::other"
for i in ${contexts}
do
   tmp=`echo "$i"  | sed 's/'-'/'_'/g' | sed 's/'::'/'_'/g'`
  copydb ${MONITOR_DB}'_'${tmp}
done