CREATE OR REPLACE VIEW ${stats_db_name}.organization_projects AS SELECT id AS project, organization as id FROM ${stats_db_name}.project_organizations;
