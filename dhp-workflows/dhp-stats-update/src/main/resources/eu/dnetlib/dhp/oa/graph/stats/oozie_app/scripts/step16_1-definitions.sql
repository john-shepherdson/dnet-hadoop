set mapred.job.queue.name=analytics; /*EOS*/

----------------------------------------------------
-- Shortcuts for various definitions in stats db ---
----------------------------------------------------

-- Peer reviewed:
drop table if exists ${stats_db_name}.result_peerreviewed purge; /*EOS*/

create table IF NOT EXISTS ${stats_db_name}.result_peerreviewed STORED AS PARQUET as
select r.id as id, case when doi.doi_from_crossref=1 and grey.grey_lit=0 then true else false end as peer_reviewed
from ${stats_db_name}.result r
left outer join ${stats_db_name}.indi_pub_doi_from_crossref doi on doi.id=r.id
left outer join ${stats_db_name}.indi_pub_grey_lit grey on grey.id=r.id; /*EOS*/

-- Green OA:
drop table if exists ${stats_db_name}.result_greenoa purge; /*EOS*/

create table IF NOT EXISTS ${stats_db_name}.result_greenoa STORED AS PARQUET as
select r.id, case when green.green_oa=1 then true else false end as green
from ${stats_db_name}.result r
left outer join ${stats_db_name}.indi_pub_green_oa green on green.id=r.id; /*EOS*/

-- GOLD OA:
drop table if exists ${stats_db_name}.result_gold purge; /*EOS*/

create table IF NOT EXISTS ${stats_db_name}.result_gold STORED AS PARQUET as
select r.id, case when gold.is_gold=1 then true else false end as gold
from ${stats_db_name}.result r
         left outer join ${stats_db_name}.indi_pub_gold_oa gold on gold.id=r.id; /*EOS*/