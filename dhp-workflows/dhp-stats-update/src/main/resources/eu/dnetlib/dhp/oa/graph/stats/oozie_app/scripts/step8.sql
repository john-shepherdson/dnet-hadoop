----------------------------------------------------
----------------------------------------------------
-- 8. Result table/view and Result related tables/views
----------------------------------------------------
----------------------------------------------------

-- Views on temporary tables that should be re-created in the end
CREATE OR REPLACE VIEW ${hive_db_name}.result as SELECT *, bestlicence as access_mode FROM ${hive_db_name}.publication_tmp UNION ALL SELECT *,bestlicence as access_mode FROM ${hive_db_name}.software_tmp UNION ALL SELECT *,bestlicence as access_mode FROM ${hive_db_name}.dataset_tmp UNION ALL SELECT *,bestlicence as access_mode FROM ${hive_db_name}.otherresearchproduct_tmp;

-- Views on final tables
CREATE OR REPLACE VIEW ${hive_db_name}.result_datasources as SELECT * FROM ${hive_db_name}.publication_datasources UNION ALL SELECT * FROM ${hive_db_name}.software_datasources UNION ALL SELECT * FROM ${hive_db_name}.dataset_datasources UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_datasources;
CREATE OR REPLACE VIEW ${hive_db_name}.result_citations as SELECT * FROM ${hive_db_name}.publication_citations UNION ALL SELECT * FROM ${hive_db_name}.software_citations UNION ALL SELECT * FROM ${hive_db_name}.dataset_citations UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_citations;
CREATE OR REPLACE VIEW ${hive_db_name}.result_classifications as SELECT * FROM ${hive_db_name}.publication_classifications UNION ALL SELECT * FROM ${hive_db_name}.software_classifications UNION ALL SELECT * FROM ${hive_db_name}.dataset_classifications UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_classifications;
CREATE OR REPLACE VIEW ${hive_db_name}.result_concepts as SELECT * FROM ${hive_db_name}.publication_concepts UNION ALL SELECT * FROM ${hive_db_name}.software_concepts UNION ALL SELECT * FROM ${hive_db_name}.dataset_concepts UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_concepts;
CREATE OR REPLACE VIEW ${hive_db_name}.result_languages as SELECT * FROM ${hive_db_name}.publication_languages UNION ALL SELECT * FROM ${hive_db_name}.software_languages UNION ALL SELECT * FROM ${hive_db_name}.dataset_languages UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_languages;
CREATE OR REPLACE VIEW ${hive_db_name}.result_oids as SELECT * FROM ${hive_db_name}.publication_oids UNION ALL SELECT * FROM ${hive_db_name}.software_oids UNION ALL SELECT * FROM ${hive_db_name}.dataset_oids UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_oids;
CREATE OR REPLACE VIEW ${hive_db_name}.result_pids as SELECT * FROM ${hive_db_name}.publication_pids UNION ALL SELECT * FROM ${hive_db_name}.software_pids UNION ALL SELECT * FROM ${hive_db_name}.dataset_pids UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_pids;
CREATE OR REPLACE VIEW ${hive_db_name}.result_topics as SELECT * FROM ${hive_db_name}.publication_topics UNION ALL SELECT * FROM ${hive_db_name}.software_topics UNION ALL SELECT * FROM ${hive_db_name}.dataset_topics UNION ALL SELECT * FROM ${hive_db_name}.otherresearchproduct_topics;

DROP TABLE IF EXISTS ${hive_db_name}.result_organization;
CREATE TABLE ${hive_db_name}.result_organization AS SELECT substr(r.target, 4) as id, substr(r.source, 4) as organization from ${hive_source_db_name}.relation r where r.reltype='resultOrganization';

DROP TABLE IF EXISTS ${hive_db_name}.result_projects;
CREATE TABLE ${hive_db_name}.result_projects AS select pr.result as id, pr.id as project, datediff(p.enddate, p.startdate) as daysfromend from ${hive_db_name}.result r join ${hive_db_name}.project_results pr on r.id=pr.result join ${hive_db_name}.project_tmp p on p.id=pr.id;
