set mapred.job.queue.name=analytics; /*EOS*/

------------------------------------------------------
------------------------------------------------------
-- Additional relations
--
-- Refereed related tables/views
------------------------------------------------------
------------------------------------------------------

DROP TABLE IF EXISTS ${stats_db_name}.publication_refereed purge; /*EOS*/
CREATE TABLE IF NOT EXISTS ${stats_db_name}.publication_refereed STORED AS PARQUET as
with peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.publication r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='peerReviewed'),
non_peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.publication r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='nonPeerReviewed')
select /*+ COALESCE(100) */ distinct *
from (
    select peer_reviewed.* from peer_reviewed
    union all
    select non_peer_reviewed.* from non_peer_reviewed
    left join peer_reviewed on peer_reviewed.id=non_peer_reviewed.id
    where peer_reviewed.id is null) pr; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_refereed purge; /*EOS*/
CREATE TABLE IF NOT EXISTS ${stats_db_name}.dataset_refereed STORED AS PARQUET as
with peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.dataset r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='peerReviewed'),
non_peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.dataset r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='nonPeerReviewed')
select /*+ COALESCE(100) */ distinct *
from (
    select peer_reviewed.* from peer_reviewed
    union all
    select non_peer_reviewed.* from non_peer_reviewed
    left join peer_reviewed on peer_reviewed.id=non_peer_reviewed.id
    where peer_reviewed.id is null) pr; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_refereed purge; /*EOS*/
CREATE TABLE IF NOT EXISTS ${stats_db_name}.software_refereed STORED AS PARQUET as
with peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.software r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='peerReviewed'),
non_peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.software r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='nonPeerReviewed')
select /*+ COALESCE(100) */ distinct *
from (
    select peer_reviewed.* from peer_reviewed
    union all
    select non_peer_reviewed.* from non_peer_reviewed
    left join peer_reviewed on peer_reviewed.id=non_peer_reviewed.id
    where peer_reviewed.id is null) pr; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_refereed purge; /*EOS*/
CREATE TABLE IF NOT EXISTS ${stats_db_name}.otherresearchproduct_refereed STORED AS PARQUET as
with peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.otherresearchproduct r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='peerReviewed'),
non_peer_reviewed as (
    select distinct substr(r.id, 4) as id, inst.refereed.classname as refereed
    from ${openaire_db_name}.otherresearchproduct r lateral view explode(r.instance) instances as inst
    where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE and inst.refereed.classname='nonPeerReviewed')
select /*+ COALESCE(100) */ distinct *
from (
    select peer_reviewed.* from peer_reviewed
    union all
    select non_peer_reviewed.* from non_peer_reviewed
    left join peer_reviewed on peer_reviewed.id=non_peer_reviewed.id
    where peer_reviewed.id is null) pr; /*EOS*/

CREATE VIEW IF NOT EXISTS ${stats_db_name}.result_refereed as
select * from ${stats_db_name}.publication_refereed
union all
select * from ${stats_db_name}.dataset_refereed
union all
select * from ${stats_db_name}.software_refereed
union all
select * from ${stats_db_name}.otherresearchproduct_refereed; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.indi_impact_measures purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_impact_measures STORED AS PARQUET as
select /*+ COALESCE(100) */ substr(id, 4) as id, measures_ids.id impactmetric, cast(measures_ids.unit.value[0] as double) score,
cast(measures_ids.unit.value[0] as decimal(6,3)) score_dec, measures_ids.unit.value[1] impact_class
from ${openaire_db_name}.result lateral view explode(measures) measures as measures_ids
where measures_ids.id!='views' and measures_ids.id!='downloads'; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.result_apc_affiliations purge; /*EOS*/

create table if not exists ${stats_db_name}.result_apc_affiliations STORED AS PARQUET as
select /*+ COALESCE(100) */ distinct substr(rel.target,4) id, substr(rel.source,4) organization, o.legalname.value name,
cast(rel.properties[0].value as double) apc_amount,
rel.properties[1].value apc_currency
from ${openaire_db_name}.relation rel
join ${openaire_db_name}.organization o on o.id=rel.source
join ${openaire_db_name}.result r on r.id=rel.target
where rel.subreltype = 'affiliation' and rel.datainfo.deletedbyinference = false and size(rel.properties)>0; /*EOS*/
