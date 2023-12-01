export PYTHON_EGG_CACHE=/home/$(whoami)/.python-eggs
export link_folder=/tmp/impala-shell-python-egg-cache-$(whoami)
if ! [ -L $link_folder ]
then
    rm -Rf "$link_folder"
    ln -sfn ${PYTHON_EGG_CACHE}${link_folder} ${link_folder}
fi

function createPDFsAggregated() {
  db=$1

impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -d ${db} -q "drop table if exists indi_is_result_accessible";

impala-shell --user $HADOOP_USER_NAME -i impala-cluster-dn1.openaire.eu -d ${db} -q "create table indi_is_result_accessible stored as parquet as
    select distinct p.id, coalesce(is_result_accessible, 0) as is_result_accessible from result p
    left outer join
    (select id, 1 as is_result_accessible from (select pl.* from result r
    join pdfaggregation_i.publication p on r.id=p.id
    join pdfaggregation_i.payload pl on pl.id=p.id
    union all
    select pl.* from result r
    join pdfaggregation_i.publication p on r.id=p.dedupid
    join pdfaggregation_i.payload pl on pl.id=p.id) foo) tmp on p.id=tmp.id";
}

STATS_DB=$1
MONITOR_DB=$2
HADOOP_USER_NAME=$3

createPDFsAggregated $STATS_DB
createPDFsAggregated $MONITOR_DB

createPDFsAggregated $MONITOR_DB'_funded'
createPDFsAggregated $MONITOR_DB'_institutions'
createPDFsAggregated $MONITOR_DB'_ris_tail'

contexts="knowmad::other dh-ch::other enermaps::other gotriple::other neanias-atmospheric::other rural-digital-europe::other covid-19::other aurora::other neanias-space::other north-america-studies::other north-american-studies::other eutopia::other"
for i in ${contexts}
do
   tmp=`echo "$i"  | sed 's/'-'/'_'/g' | sed 's/'::'/'_'/g'`
  createPDFsAggregated ${MONITOR_DB}'_'${tmp}
done