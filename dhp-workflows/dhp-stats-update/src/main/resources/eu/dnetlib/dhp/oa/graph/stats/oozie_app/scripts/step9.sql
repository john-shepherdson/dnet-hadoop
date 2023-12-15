----------------------------------------------------------------
----------------------------------------------------------------
-- Organization table/view and Organization related tables/views
----------------------------------------------------------------
----------------------------------------------------------------
DROP TABLE IF EXISTS ${stats_db_name}.organization purge;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.organization STORED AS PARQUET AS
SELECT substr(o.id, 4)        as id,
       o.legalname.value      as name,
       o.legalshortname.value as legalshortname,
       o.country.classid      as country
FROM ${openaire_db_name}.organization o
WHERE o.datainfo.deletedbyinference = FALSE and o.datainfo.invisible = FALSE;

CREATE OR REPLACE VIEW ${stats_db_name}.organization_datasources AS
SELECT organization AS id, id AS datasource
FROM ${stats_db_name}.datasource_organizations;

CREATE OR REPLACE VIEW ${stats_db_name}.organization_projects AS
SELECT id AS project, organization as id
FROM ${stats_db_name}.project_organizations;