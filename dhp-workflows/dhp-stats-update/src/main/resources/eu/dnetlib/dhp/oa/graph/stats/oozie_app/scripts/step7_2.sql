-- Project_organizations Table
DROP TABLE IF EXISTS ${stats_db_name}.project_organizations;
CREATE TABLE ${stats_db_name}.project_organizations AS SELECT substr(r.source, 4) as id, substr(r.target, 4) AS organization from ${openaire_db_name}.relation r WHERE r.reltype='projectOrganization';
