----------------------------------------------------------------
----------------------------------------------------------------
-- Organization table/view and Organization related tables/views
----------------------------------------------------------------
----------------------------------------------------------------
DROP TABLE IF EXISTS ${hive_db_name}.organization;
CREATE TABLE ${hive_db_name}.organization AS SELECT substr(o.id, 4) as id, o.legalname.value as name, o.country.classid as country from ${hive_source_db_name}.organization o WHERE o.datainfo.deletedbyinference=false;

CREATE OR REPLACE VIEW ${hive_db_name}.organization_datasources AS SELECT organization AS id, id AS datasource FROM ${hive_db_name}.datasource_organizations;
CREATE OR REPLACE VIEW ${hive_db_name}.organization_projects AS SELECT id AS project, organization as id FROM ${hive_db_name}.project_organizations;
