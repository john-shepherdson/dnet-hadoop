DROP TABLE IF EXISTS ${stats_db_name}.datasource_organizations;
CREATE TABLE ${stats_db_name}.datasource_organizations AS select substr(r.target, 4) as id, substr(r.source, 4) as organization from ${openaire_db_name}.relation r where r.reltype='datasourceOrganization';
