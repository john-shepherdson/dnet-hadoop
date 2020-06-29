----------------------------------------------------
-- Shortcuts for various definitions in stats db ---
----------------------------------------------------


-- Peer reviewed:
-- Results that have been collected from Crossref
create table ${stats_db_name}.result_peerreviewed as
with peer_reviewed as (
    select distinct r.id as id
    from ${stats_db_name}.result r
    join ${stats_db_name}.result_sources rs on rs.id=r.id
    join ${stats_db_name}.datasource d on d.id=rs.datasource
    where d.name='Crossref')
select distinct peer_reviewed.id as id, true as peer_reviewed
from peer_reviewed
union all
select distinct r.id as id, false as peer_reviewed
from ${stats_db_name}.result r
where r.id not in (select id from peer_reviewed);

-- Green OA:
-- OA results that are hosted by an Institutional repository and have NOT been harvested from a DOAJ journal.
create table ${stats_db_name}.result_greenoa as
with result_green as (
    select distinct r.id as id
    from ${stats_db_name}.result r
    join ${stats_db_name}.result_datasources rd on rd.id=r.id
    join ${stats_db_name}.datasource d on d.id=rd.datasource
    where r.bestlicence in ('Open Access', 'Open Source') and d.type='Institutional Repository' and not exists (
        select 1 from ${stats_db_name}.result_datasources rd
        join ${stats_db_name}.datasource d on rd.datasource=d.id
        join ${stats_db_name}.datasource_sources sds on sds.id=d.id
        join ${stats_db_name}.datasource sd on sd.id=sds.datasource
        where sd.name='DOAJ-ARTICLES' and rd.id=r.id))
select distinct result_green.id, true as green
from result_green
union all
select distinct r.id as id, false as green
from ${stats_db_name}.result r
where r.id not in (select id from result_green);

-- GOLD OA:
-- OA results that have been harvested from a DOAJ journal.
create table ${stats_db_name}.result_gold as
with result_gold as (
    select distinct r.id as id
    from ${stats_db_name}.result r
    join ${stats_db_name}.result_datasources rd on rd.id=r.id
    join ${stats_db_name}.datasource d on d.id=rd.datasource
    join ${stats_db_name}.datasource_sources sds on sds.id=d.id
    join ${stats_db_name}.datasource sd on sd.id=sds.datasource
    where r.type='publication' and r.bestlicence='Open Access' and sd.name='DOAJ-Articles')
select distinct result_gold.id, true as gold
from result_gold
union all
select distinct r.id, false as gold
from ${stats_db_name}.result r
where r.id not in (select id from result_gold);

-- shortcut result-country through the organization affiliation
create table ${stats_db_name}.result_affiliated_country as
select r.id as id, o.country as country
from ${stats_db_name}.result r
join ${stats_db_name}.result_organization ro on ro.id=r.id
join ${stats_db_name}.organization o on o.id=ro.organization
where o.country is not null and o.country!='';

-- shortcut result-country through datasource of deposition
create table ${stats_db_name}.result_deposited_country as
select r.id as id, o.country as country
from ${stats_db_name}.result r
join ${stats_db_name}.result_datasources rd on rd.id=r.id
join ${stats_db_name}.datasource d on d.id=rd.datasource
join ${stats_db_name}.datasource_organizations dor on dor.id=d.id
join ${stats_db_name}.organization o on o.id=dor.organization
where o.country is not null and o.country!='';

-- replace the creation of the result view to include the boolean fields from the previous tables (green, gold,
-- peer reviewed)
drop table if exists ${stats_db_name}.result_tmp;
CREATE TABLE result_tmp (
    id STRING,
    title STRING,
    publisher STRING,
    journal STRING,
    `date` STRING,
    `year` INT,
    bestlicence STRING,
    access_mode STRING,
    embargo_end_date STRING,
    delayed BOOLEAN,
    authors INT,
    source STRING,
    abstract BOOLEAN,
    type STRING ,
    peer_reviewed BOOLEAN,
    green BOOLEAN,
    gold BOOLEAN)
clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.publication r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id;

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.dataset r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id;

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.software r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id;

insert into ${stats_db_name}.result_tmp
select r.id, r.title, r.publisher, r.journal, r.`date`, date_format(r.`date`, 'yyyy'), r.bestlicence, r.bestlicence, r.embargo_end_date, r.delayed, r.authors, r.source, r.abstract, r.type, pr.peer_reviewed, green.green, gold.gold
FROM ${stats_db_name}.otherresearchproduct r
LEFT OUTER JOIN ${stats_db_name}.result_peerreviewed pr on pr.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_greenoa green on green.id=r.id
LEFT OUTER JOIN ${stats_db_name}.result_gold gold on gold.id=r.id;

drop table if exists ${stats_db_name}.result;
drop view if exists ${stats_db_name}.result;
create table ${stats_db_name}.result stored as parquet as select * from ${stats_db_name}.result_tmp;
drop table ${stats_db_name}.result_tmp;