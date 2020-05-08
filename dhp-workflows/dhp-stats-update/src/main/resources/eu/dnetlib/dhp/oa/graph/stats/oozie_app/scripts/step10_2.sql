CREATE OR REPLACE VIEW ${stats_db_name}.organization_datasources AS SELECT organization AS id, id AS datasource FROM ${stats_db_name}.datasource_organizations;
