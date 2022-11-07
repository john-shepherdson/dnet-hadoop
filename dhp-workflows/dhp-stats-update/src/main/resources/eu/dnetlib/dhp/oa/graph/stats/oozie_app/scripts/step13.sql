------------------------------------------------------
------------------------------------------------------
-- Additional relations
--
-- Sources related tables/views
------------------------------------------------------
------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${stats_db_name}.publication_sources STORED AS PARQUET as
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.publication p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false and d.datainfo.invisible = FALSE) d on p.datasource = d.id;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.dataset_sources STORED AS PARQUET as
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.dataset p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false and d.datainfo.invisible = FALSE) d on p.datasource = d.id;
    
CREATE TABLE IF NOT EXISTS ${stats_db_name}.software_sources STORED AS PARQUET as
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.software p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false and d.datainfo.invisible = FALSE) d on p.datasource = d.id;
    
CREATE TABLE IF NOT EXISTS ${stats_db_name}.otherresearchproduct_sources STORED AS PARQUET as
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.otherresearchproduct p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false and d.datainfo.invisible = FALSE) d on p.datasource = d.id;
    
CREATE VIEW IF NOT EXISTS ${stats_db_name}.result_sources AS
SELECT * FROM ${stats_db_name}.publication_sources
UNION ALL
SELECT * FROM ${stats_db_name}.dataset_sources
UNION ALL
SELECT * FROM ${stats_db_name}.software_sources
UNION ALL
SELECT * FROM ${stats_db_name}.otherresearchproduct_sources;


CREATE TABLE IF NOT EXISTS ${stats_db_name}.result_orcid STORED AS PARQUET as
select distinct res.id, regexp_replace(res.orcid, 'http://orcid.org/' ,'') as orcid
from (
    SELECT substr(res.id, 4) as id, auth_pid.value as orcid
    FROM ${openaire_db_name}.result res
    LATERAL VIEW explode(author) a as auth
    LATERAL VIEW explode(auth.pid) ap as auth_pid
    LATERAL VIEW explode(auth.pid.qualifier.classid) apt as author_pid_type
    WHERE res.datainfo.deletedbyinference = FALSE and res.datainfo.invisible = FALSE and author_pid_type = 'orcid') as res;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.result_result stored as parquet as
select substr(rel.source, 4) as source, substr(rel.target, 4) as target, relclass, subreltype
from ${openaire_db_name}.relation rel
join ${openaire_db_name}.result r1 on rel.source=r1.id
join ${openaire_db_name}.result r2 on r2.id=rel.target
where reltype='resultResult'
    and r1.resulttype.classname!=r2.resulttype.classname
    and r1.datainfo.deletedbyinference=false and r1.datainfo.invisible = FALSE
    and r2.datainfo.deletedbyinference=false and r2.datainfo.invisible = FALSE
    and r1.resulttype.classname != 'other'
    and r2.resulttype.classname != 'other'
    and rel.datainfo.deletedbyinference=false and rel.datainfo.invisible = FALSE;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.result_citations_oc stored as parquet as
select substr(target, 4) as id, count(distinct substr(source, 4)) as citations
from ${openaire_db_name}.relation rel
join ${openaire_db_name}.result r1 on rel.source=r1.id
join ${openaire_db_name}.result r2 on r2.id=rel.target
where relClass='Cites' and rel.datainfo.provenanceaction.classid = 'sysimport:crosswalk:opencitations'
    and reltype='resultResult'
    and r1.resulttype.classname!=r2.resulttype.classname
    and r1.datainfo.deletedbyinference=false and r1.datainfo.invisible = FALSE
    and r2.datainfo.deletedbyinference=false and r2.datainfo.invisible = FALSE
    and r1.resulttype.classname != 'other'
    and r2.resulttype.classname != 'other'
    and rel.datainfo.deletedbyinference=false and rel.datainfo.invisible = FALSE
group by substr(target, 4);

CREATE TABLE IF NOT EXISTS ${stats_db_name}.result_references_oc stored as parquet as
select substr(source, 4) as id, count(distinct substr(target, 4)) as references
from ${openaire_db_name}.relation rel
         join ${openaire_db_name}.result r1 on rel.source=r1.id
         join ${openaire_db_name}.result r2 on r2.id=rel.target
where relClass='Cites' and rel.datainfo.provenanceaction.classid = 'sysimport:crosswalk:opencitations'
  and reltype='resultResult'
  and r1.resulttype.classname!=r2.resulttype.classname
    and r1.datainfo.deletedbyinference=false and r1.datainfo.invisible = FALSE
    and r2.datainfo.deletedbyinference=false and r2.datainfo.invisible = FALSE
    and r1.resulttype.classname != 'other'
    and r2.resulttype.classname != 'other'
    and rel.datainfo.deletedbyinference=false and rel.datainfo.invisible = FALSE
group by substr(source, 4);