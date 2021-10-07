----------------------------------------------------
-- Shortcuts for various definitions in stats db ---
----------------------------------------------------

-- Peer reviewed:
create table ${stats_db_name}.result_peerreviewed as
select r.id as id, case when doi.doi_from_crossref=1 and grey.grey_lit=0 then true else false end as peer_reviewed
from ${stats_db_name}.result r
left outer join ${stats_db_name}.indi_pub_doi_from_crossref doi on doi.id=r.id
left outer join ${stats_db_name}.indi_pub_grey_lit grey on grey.id=r.id;

-- Green OA:
create table ${stats_db_name}.result_greenoa as
select r.id, case when green.green_oa=1 then true else false end as green
from ${stats_db_name}.result r
left outer join ${stats_db_name}.indi_pub_green_oa green on green.id=r.id;

-- GOLD OA:
create table ${stats_db_name}.result_gold as
select r.id, case when gold.gold_oa=1 then true else false end as gold
from ${stats_db_name}.result r
         left outer join ${stats_db_name}.indi_pub_gold_oa gold on gold.id=r.id;