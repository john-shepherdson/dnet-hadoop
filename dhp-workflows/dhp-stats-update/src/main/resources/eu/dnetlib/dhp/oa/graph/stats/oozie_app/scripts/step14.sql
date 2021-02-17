------------------------------------------------------
------------------------------------------------------
-- Additional relations
--
-- Licences related tables/views
------------------------------------------------------
------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${stats_db_name}.publication_licenses AS
SELECT substr(p.id, 4) as id, licenses.value as type 
from ${openaire_db_name}.publication p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.dataset_licenses AS
SELECT substr(p.id, 4) as id, licenses.value as type 
from ${openaire_db_name}.dataset p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.software_licenses AS
SELECT substr(p.id, 4) as id, licenses.value as type 
from ${openaire_db_name}.software p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.otherresearchproduct_licenses AS
SELECT substr(p.id, 4) as id, licenses.value as type 
from ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.instance.license) instances as licenses
where licenses.value is not null and licenses.value != '' and p.datainfo.deletedbyinference=false;

CREATE VIEW IF NOT EXISTS ${stats_db_name}.result_licenses AS
SELECT * FROM ${stats_db_name}.publication_licenses
UNION ALL
SELECT * FROM ${stats_db_name}.dataset_licenses
UNION ALL
SELECT * FROM ${stats_db_name}.software_licenses
UNION ALL
SELECT * FROM ${stats_db_name}.otherresearchproduct_licenses;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.organization_pids AS 
select substr(o.id, 4) as id, ppid.qualifier.classname as type, ppid.value as pid 
from ${openaire_db_name}.organization o lateral view explode(o.pid) pids as ppid;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.organization_sources as 
SELECT o.id, case when d.id is null then 'other' else o.datasource end as datasource 
FROM (
    SELECT  substr(o.id, 4) as id, substr(instances.instance.key, 4) as datasource 
    from ${openaire_db_name}.organization o lateral view explode(o.collectedfrom) instances as instance) o 
    LEFT OUTER JOIN (
        SELECT substr(d.id, 4) id 
        from ${openaire_db_name}.datasource d 
        WHERE d.datainfo.deletedbyinference=false) d on o.datasource = d.id;

ANALYZE TABLE ${stats_db_name}.publication_licenses COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.publication_licenses COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_licenses COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_licenses COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.software_licenses COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.software_licenses COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.otherresearchproduct_licenses COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.otherresearchproduct_licenses COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.organization_pids COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.organization_pids COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.organization_sources COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.organization_sources COMPUTE STATISTICS FOR COLUMNS;