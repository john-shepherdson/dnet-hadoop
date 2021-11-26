-------------------------------------------
--- Extra tables, mostly used by indicators

create table ${stats_db_name}.result_projectcount STORED AS PARQUET as
select r.id, count(distinct p.id) as count
from ${stats_db_name}.result r
left outer join ${stats_db_name}.result_projects rp on rp.id=r.id
left outer join ${stats_db_name}.project p on p.id=rp.project
group by r.id;

create table ${stats_db_name}.result_fundercount STORED AS PARQUET as
select r.id, count(distinct p.funder) as count
from ${stats_db_name}.result r
left outer join ${stats_db_name}.result_projects rp on rp.id=r.id
left outer join ${stats_db_name}.project p on p.id=rp.project
group by r.id;

create table ${stats_db_name}.project_resultcount STORED AS PARQUET as
with rcount as (
    select p.id as pid, count(distinct r.id) as `count`, r.type as type
    from ${stats_db_name}.project p
    left outer join ${stats_db_name}.result_projects rp on rp.project=p.id
    left outer join ${stats_db_name}.result r on r.id=rp.id
    group by r.type, p.id )
select rcount.pid, sum(case when rcount.type='publication' then rcount.count else 0 end) as publications,
    sum(case when rcount.type='dataset' then rcount.count else 0 end) as datasets,
    sum(case when rcount.type='software' then rcount.count else 0 end) as software,
    sum(case when rcount.type='other' then rcount.count else 0 end) as other
from rcount
group by rcount.pid;

create table ${stats_db_name}.result_instance stored as parquet as
select distinct r.*
from (
         select substr(r.id, 4) as id, inst.accessright.classname as accessright, substr(inst.collectedfrom.key, 4) as collectedfrom,
                substr(inst.hostedby.key, 4) as hostedby, inst.dateofacceptance.value as dateofacceptance, inst.license.value as license, p.qualifier.classname as pidtype, p.value as pid
         from ${openaire_db_name}.result r lateral view explode(r.instance) instances as inst lateral view explode(inst.pid) pids as p) r
join ${stats_db_name}.result res on res.id=r.id;

create table ${stats_db_name}.result_apc STORED AS PARQUET as
select r.id, r.amount, r.currency
from (
         select substr(r.id, 4) as id, inst.processingchargeamount.value as amount, inst.processingchargecurrency.value as currency
         from ${openaire_db_name}.result r lateral view explode(r.instance) instances as inst) r
join ${stats_db_name}.result res on res.id=r.id
where r.amount is not null;