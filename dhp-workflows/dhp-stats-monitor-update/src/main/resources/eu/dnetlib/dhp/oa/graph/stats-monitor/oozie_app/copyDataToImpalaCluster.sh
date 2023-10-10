export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

#export HADOOP_USER_NAME=$2

function copydb() {

  export HADOOP_USER="dimitris.pierrakos"
  export HADOOP_USER_NAME='dimitris.pierrakos'

  db=$1
  FILE=("hive_wf_tmp_"$RANDOM)
  hdfs dfs -mkdir hdfs://impala-cluster-mn1.openaire.eu:8020/tmp/$FILE/

  # change ownership to impala
#  hdfs dfs -conf /etc/impala_cluster/hdfs-site.xml -chmod -R 777 /tmp/$FILE/${db}.db
  hdfs dfs -conf /etc/impala_cluster/hdfs-site.xml -chmod -R 777 /tmp/$FILE/


  # copy the databases from ocean to impala
  echo "copying $db"
  hadoop distcp -Dmapreduce.map.memory.mb=6144 -pb hdfs://nameservice1/user/hive/warehouse/${db}.db hdfs://impala-cluster-mn1.openaire.eu:8020/tmp/$FILE/

  hdfs dfs -conf /etc/impala_cluster/hdfs-site.xml -chmod -R 777 /tmp/$FILE/${db}.db

  # drop tables from db
  for i in `impala-shell -i impala-cluster-dn1.openaire.eu -d ${db} --delimited  -q "show tables"`;
    do
        `impala-shell -i impala-cluster-dn1.openaire.eu -d ${db} -q "drop table $i;"`;
    done

  # drop views from db
  for i in `impala-shell -i impala-cluster-dn1.openaire.eu -d ${db} --delimited  -q "show tables"`;
    do
        `impala-shell  -i impala-cluster-dn1.openaire.eu -d ${db} -q "drop view $i;"`;
    done

  # delete the database
  impala-shell -i impala-cluster-dn1.openaire.eu -q "drop database if exists ${db} cascade";

  # create the databases
  impala-shell -i impala-cluster-dn1.openaire.eu -q "create database ${db}";

  impala-shell -q "INVALIDATE METADATA"
  echo "creating schema for ${db}"
  for ((  k  = 0;  k  < 5;  k ++ )); do
  for i in `impala-shell -d ${db} --delimited  -q "show tables"`;
    do
      impala-shell -d ${db} --delimited  -q "show create table $i";
    done |  sed 's/"$/;/' | sed 's/^"//' | sed 's/[[:space:]]\date[[:space:]]/`date`/g' | impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -c -f -
  done

  # load the data from /tmp in the respective tables
  echo "copying data in tables and computing stats"
  for i in `impala-shell -i impala-cluster-dn1.openaire.eu -d ${db} --delimited  -q "show tables"`;
      do
        impala-shell -i impala-cluster-dn1.openaire.eu -d ${db} -q "load data inpath '/tmp/$FILE/${db}.db/$i' into table $i";
        impala-shell -i impala-cluster-dn1.openaire.eu -d ${db} -q "compute stats $i";
      done

  # deleting the remaining directory from hdfs
hdfs dfs -conf /etc/impala_cluster/hdfs-site.xml -rm -R /tmp/$FILE/${db}.db
}

MONITOR_DB=$1
#HADOOP_USER_NAME=$2

copydb $MONITOR_DB'_institutions'
copydb $MONITOR_DB

