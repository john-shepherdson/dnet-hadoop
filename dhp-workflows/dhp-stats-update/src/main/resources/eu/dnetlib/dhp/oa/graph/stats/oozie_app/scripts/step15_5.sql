set mapred.job.queue.name=analytics; /*EOS*/

-------------------------------------------
--- Extra tables, mostly used by indicators

DROP TABLE IF EXISTS ${stats_db_name}.result_projectcount purge; /*EOS*/

create table if not exists ${stats_db_name}.result_projectcount STORED AS PARQUET as
select /*+ COALESCE(100) */ r.id, count(distinct rp.project) as count
from ${stats_db_name}.result r
left outer join ${stats_db_name}.result_projects rp on rp.id=r.id
group by r.id; /*EOS*/

ANALYZE TABLE ${stats_db_name}.result_projectcount COMPUTE STATISTICS; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.project_resultcount purge; /*EOS*/

create table if not exists ${stats_db_name}.project_res stored as parquet as
select distinct r.id as res, r.type, p.id as pid
from ${stats_db_name}.project p
left outer join ${stats_db_name}.result_projects rp on rp.project=p.id
left outer join ${stats_db_name}.result r on r.id=rp.id; /*EOS*/

ANALYZE TABLE ${stats_db_name}.project_res COMPUTE STATISTICS; /*EOS*/


create table if not exists ${stats_db_name}.project_resultcount STORED AS PARQUET as
select pid,
       sum(case when rp.type='publication' then 1 else 0 end) as publications,
       sum(case when rp.type='dataset' then 1 else 0 end) as datasets,
       sum(case when rp.type='software' then 1 else 0 end) as software,
       sum(case when rp.type='other' then 1 else 0 end) as other
from ${stats_db_name}.project_res
group by pid; /*EOS*/

ANALYZE TABLE ${stats_db_name}.project_resultcount COMPUTE STATISTICS; /*EOS*/

drop table ${stats_db_name}.project_res; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.result_fundercount purge; /*EOS*/
drop table if exists ${stats_db_name}.result_funder purge; /*EOS*/

create table if not exists ${stats_db_name}.result_funder stored as parquet as
select distinct rp.id, p.funder
from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project; /*EOS*/

ANALYZE TABLE ${stats_db_name}.result_funder COMPUTE STATISTICS; /*EOS*/

create table if not exists ${stats_db_name}.result_fundercount STORED AS PARQUET as
select /*+ COALESCE(100) */ r.id, count(rf.funder) as count
from ${stats_db_name}.result r
left outer join ${stats_db_name}.result_funder rf on rf.id=r.id
group by r.id; /*EOS*/

ANALYZE TABLE ${stats_db_name}.result_fundercount COMPUTE STATISTICS; /*EOS*/

drop table ${stats_db_name}.result_funder; /*EOS*/

create or replace view ${stats_db_name}.rndexpenditure as select * from stats_ext.rndexpediture; /*EOS*/
create or replace view ${stats_db_name}.rndgdpexpenditure as select * from stats_ext.rndgdpexpenditure; /*EOS*/
create or replace view ${stats_db_name}.doctoratestudents as select * from stats_ext.doctoratestudents; /*EOS*/
create or replace view ${stats_db_name}.totalresearchers as select * from stats_ext.totalresearchers; /*EOS*/
create or replace view ${stats_db_name}.totalresearchersft as select * from stats_ext.totalresearchersft; /*EOS*/
create or replace view ${stats_db_name}.hrrst as select * from stats_ext.hrrst; /*EOS*/
create or replace view ${stats_db_name}.graduatedoctorates as select * from stats_ext.graduatedoctorates; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.result_instance purge; /*EOS*/

create table if not exists ${stats_db_name}.result_instance stored as parquet as
select /*+ COALESCE(100) */ distinct r.*
from (
         select substr(r.id, 4) as id, inst.accessright.classname as accessright, inst.accessright.openaccessroute as accessright_uw, substr(inst.collectedfrom.key, 4) as collectedfrom,
                substr(inst.hostedby.key, 4) as hostedby, inst.dateofacceptance.value as dateofacceptance, inst.license.value as license, p.qualifier.classname as pidtype, p.value as pid
         from ${openaire_db_name}.result r lateral view explode(r.instance) instances as inst lateral view outer explode(inst.pid) pids as p) r
join ${stats_db_name}.result res on res.id=r.id; /*EOS*/

ANALYZE TABLE ${stats_db_name}.result_instance COMPUTE STATISTICS; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.result_apc purge; /*EOS*/

create table if not exists ${stats_db_name}.result_apc STORED AS PARQUET as
select /*+ COALESCE(100) */ distinct r.id, r.amount, r.currency
from (
         select substr(r.id, 4) as id, cast(inst.processingchargeamount.value as float) as amount, inst.processingchargecurrency.value as currency
         from ${openaire_db_name}.result r lateral view explode(r.instance) instances as inst) r
join ${stats_db_name}.result res on res.id=r.id
where r.amount is not null; /*EOS*/

ANALYZE TABLE ${stats_db_name}.result_apc COMPUTE STATISTICS; /*EOS*/

create or replace view ${stats_db_name}.issn_gold_oa_dataset as select * from ${external_stats_db_name}.issn_gold_oa_dataset; /*EOS*/