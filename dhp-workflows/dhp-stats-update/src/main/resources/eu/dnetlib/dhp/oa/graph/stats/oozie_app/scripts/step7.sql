----------------------------------------------------
----------------------------------------------------
-- Result table/view and Result related tables/views
----------------------------------------------------
----------------------------------------------------

-- Views on temporary tables that should be re-created in the end
CREATE OR REPLACE VIEW ${stats_db_name}.result as
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.publication_tmp
UNION ALL
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.software_tmp
UNION ALL
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.dataset_tmp
UNION ALL
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.otherresearchproduct_tmp;

-- Views on final tables
CREATE OR REPLACE VIEW ${stats_db_name}.result_datasources AS
SELECT *
FROM ${stats_db_name}.publication_datasources
UNION ALL
SELECT *
FROM ${stats_db_name}.software_datasources
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_datasources
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_datasources;

CREATE OR REPLACE VIEW ${stats_db_name}.result_citations AS
SELECT *
FROM ${stats_db_name}.publication_citations
UNION ALL
SELECT *
FROM ${stats_db_name}.software_citations
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_citations
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_citations;

CREATE OR REPLACE VIEW ${stats_db_name}.result_classifications AS
SELECT *
FROM ${stats_db_name}.publication_classifications
UNION ALL
SELECT *
FROM ${stats_db_name}.software_classifications
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_classifications
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_classifications;

CREATE OR REPLACE VIEW ${stats_db_name}.result_concepts AS
SELECT *
FROM ${stats_db_name}.publication_concepts
UNION ALL
SELECT *
FROM ${stats_db_name}.software_concepts
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_concepts
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_concepts;

CREATE OR REPLACE VIEW ${stats_db_name}.result_languages AS
SELECT *
FROM ${stats_db_name}.publication_languages
UNION ALL
SELECT *
FROM ${stats_db_name}.software_languages
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_languages
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_languages;

CREATE OR REPLACE VIEW ${stats_db_name}.result_oids AS
SELECT *
FROM ${stats_db_name}.publication_oids
UNION ALL
SELECT *
FROM ${stats_db_name}.software_oids
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_oids
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_oids;

CREATE OR REPLACE VIEW ${stats_db_name}.result_pids AS
SELECT *
FROM ${stats_db_name}.publication_pids
UNION ALL
SELECT *
FROM ${stats_db_name}.software_pids
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_pids
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_pids;

CREATE OR REPLACE VIEW ${stats_db_name}.result_topics AS
SELECT *
FROM ${stats_db_name}.publication_topics
UNION ALL
SELECT *
FROM ${stats_db_name}.software_topics
UNION ALL
SELECT *
FROM ${stats_db_name}.dataset_topics
UNION ALL
SELECT *
FROM ${stats_db_name}.otherresearchproduct_topics;

CREATE TABLE ${stats_db_name}.result_organization STORED AS PARQUET AS
SELECT substr(r.target, 4) AS id, substr(r.source, 4) AS organization
FROM ${openaire_db_name}.relation r
WHERE r.reltype = 'resultOrganization'
  and r.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.result_projects STORED AS PARQUET AS
select pr.result AS id, pr.id AS project, datediff(p.enddate, p.startdate) AS daysfromend, pr.provenance as provenance
FROM ${stats_db_name}.result r
         JOIN ${stats_db_name}.project_results pr ON r.id = pr.result
         JOIN ${stats_db_name}.project_tmp p ON p.id = pr.id;