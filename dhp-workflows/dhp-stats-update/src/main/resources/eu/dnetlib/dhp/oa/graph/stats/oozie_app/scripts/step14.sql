set mapred.job.queue.name=analytics;
------------------------------------------------------
------------------------------------------------------
-- Additional relations
--
-- Licences related tables/views
------------------------------------------------------
------------------------------------------------------
DROP TABLE IF EXISTS ${stats_db_name}.publication_licenses purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.publication_licenses STORED AS PARQUET AS
SELECT substr(p.id, 4) as id, licenses.value as type
from ${openaire_db_name}.publication p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false and p.datainfo.invisible = FALSE;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_licenses purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.dataset_licenses STORED AS PARQUET AS
SELECT substr(p.id, 4) as id, licenses.value as type 
from ${openaire_db_name}.dataset p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false and p.datainfo.invisible = FALSE;

DROP TABLE IF EXISTS ${stats_db_name}.software_licenses purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.software_licenses STORED AS PARQUET AS
SELECT substr(p.id, 4) as id, licenses.value as type 
from ${openaire_db_name}.software p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false and p.datainfo.invisible = FALSE;

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_licenses purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.otherresearchproduct_licenses STORED AS PARQUET AS
SELECT substr(p.id, 4) as id, licenses.value as type 
from ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false and p.datainfo.invisible = FALSE;

CREATE VIEW IF NOT EXISTS ${stats_db_name}.result_licenses AS
SELECT * FROM ${stats_db_name}.publication_licenses
UNION ALL
SELECT * FROM ${stats_db_name}.dataset_licenses
UNION ALL
SELECT * FROM ${stats_db_name}.software_licenses
UNION ALL
SELECT * FROM ${stats_db_name}.otherresearchproduct_licenses;

DROP TABLE IF EXISTS ${stats_db_name}.organization_pids purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.organization_pids STORED AS PARQUET AS
select substr(o.id, 4) as id, ppid.qualifier.classname as type, ppid.value as pid 
from ${openaire_db_name}.organization o lateral view explode(o.pid) pids as ppid;

DROP TABLE IF EXISTS ${stats_db_name}.organization_sources purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.organization_sources STORED AS PARQUET as
SELECT o.id, case when d.id is null then 'other' else o.datasource end as datasource 
FROM (
    SELECT  substr(o.id, 4) as id, substr(instances.instance.key, 4) as datasource 
    from ${openaire_db_name}.organization o lateral view explode(o.collectedfrom) instances as instance) o 
    LEFT OUTER JOIN (
        SELECT substr(d.id, 4) id 
        from ${openaire_db_name}.datasource d 
        WHERE d.datainfo.deletedbyinference=false and d.datainfo.invisible = FALSE) d on o.datasource = d.id;

DROP TABLE IF EXISTS ${stats_db_name}.result_accessroute purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.result_accessroute STORED AS PARQUET as
select distinct substr(id,4) as id, accessroute from ${openaire_db_name}.result
lateral view explode (instance.accessright.openaccessroute) openaccessroute as accessroute;
