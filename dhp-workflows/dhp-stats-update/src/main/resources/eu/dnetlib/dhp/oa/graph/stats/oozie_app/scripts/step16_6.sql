-------------------------------------------
--- Extra tables, mostly used by indicators

create table ${stats_db_name}.result_projectcount as
select r.id, count(distinct p.id) as count
from ${stats_db_name}.result r
left outer join ${stats_db_name}.result_projects rp on rp.id=r.id
left outer join ${stats_db_name}.project p on p.id=rp.project
group by r.id;

create table ${stats_db_name}.result_fundercount as
select r.id, count(distinct p.funder) as count
from ${stats_db_name}.result r
left outer join ${stats_db_name}.result_projects rp on rp.id=r.id
left outer join ${stats_db_name}.project p on p.id=rp.project
group by r.id;

create table ${stats_db_name}.project_resultcount as
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

create view ${stats_db_name}.rndexpenditure as select * from stats_ext.rndexpediture;
--
-- ANALYZE TABLE ${stats_db_name}.result_projectcount COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.result_projectcount COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.result_fundercount COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.result_fundercount COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.project_resultcount COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.project_resultcount COMPUTE STATISTICS FOR COLUMNS;