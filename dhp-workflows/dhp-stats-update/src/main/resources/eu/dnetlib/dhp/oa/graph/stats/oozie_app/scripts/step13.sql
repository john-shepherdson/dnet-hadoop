------------------------------------------------------
------------------------------------------------------
-- Additional relations
--
-- Sources related tables/views
------------------------------------------------------
------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${stats_db_name}.publication_sources as 
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.publication p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.dataset_sources as 
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.dataset p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;
    
CREATE TABLE IF NOT EXISTS ${stats_db_name}.software_sources as 
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.software p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;
    
CREATE TABLE IF NOT EXISTS ${stats_db_name}.otherresearchproduct_sources as 
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource 
FROM (
    SELECT  substr(p.id, 4) as id, substr(datasource, 4) as datasource 
from ${openaire_db_name}.otherresearchproduct p lateral view explode(p.collectedfrom.key) c as datasource) p 
LEFT OUTER JOIN
(
    SELECT substr(d.id, 4) id 
    from ${openaire_db_name}.datasource d 
    WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;
    
CREATE VIEW IF NOT EXISTS ${stats_db_name}.result_sources AS
SELECT * FROM ${stats_db_name}.publication_sources
UNION ALL
SELECT * FROM ${stats_db_name}.dataset_sources
UNION ALL
SELECT * FROM ${stats_db_name}.software_sources
UNION ALL
SELECT * FROM ${stats_db_name}.otherresearchproduct_sources;

ANALYZE TABLE ${stats_db_name}.publication_sources COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.publication_sources COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_sources COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_sources COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.software_sources COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.software_sources COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.otherresearchproduct_sources COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.otherresearchproduct_sources COMPUTE STATISTICS FOR COLUMNS;