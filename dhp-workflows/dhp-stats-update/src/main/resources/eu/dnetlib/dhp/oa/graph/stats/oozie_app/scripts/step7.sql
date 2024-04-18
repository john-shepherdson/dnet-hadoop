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
FROM ${stats_db_name}.otherresearchproduct_tmp; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_datasources; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_citations; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_classifications; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_concepts; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_languages; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_oids; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_pids; /*EOS*/

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
FROM ${stats_db_name}.otherresearchproduct_topics; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.result_fos purge; /*EOS*/

create table ${stats_db_name}.result_fos stored as parquet as
with
    lvl1 as (select id, topic from ${stats_db_name}.result_topics where topic like '__ %' and type='Fields of Science and Technology classification'),
    lvl2 as (select id, topic from ${stats_db_name}.result_topics where topic like '____ %' and type='Fields of Science and Technology classification'),
    lvl3 as (select id, topic from ${stats_db_name}.result_topics where topic like '______ %' and type='Fields of Science and Technology classification')
select /*+ COALESCE(100) */ lvl1.id, lvl1.topic as lvl1, lvl2.topic as lvl2, lvl3.topic as lvl3
from lvl1
 join lvl2 on lvl1.id=lvl2.id and substr(lvl2.topic, 1, 2)=substr(lvl1.topic, 1, 2)
 join lvl3 on lvl3.id=lvl1.id and substr(lvl3.topic, 1, 4)=substr(lvl2.topic, 1, 4); /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.result_organization purge; /*EOS*/

CREATE TABLE ${stats_db_name}.result_organization STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(r.target, 4) AS id, substr(r.source, 4) AS organization
FROM ${openaire_db_name}.relation r
WHERE r.reltype = 'resultOrganization'
  and r.target like '50|%'
  and r.datainfo.deletedbyinference = false and r.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.result_projects purge; /*EOS*/

CREATE TABLE ${stats_db_name}.result_projects STORED AS PARQUET AS
select /*+ COALESCE(100) */ pr.result AS id, pr.id AS project, datediff(p.enddate, p.startdate) AS daysfromend, pr.provenance as provenance
FROM ${stats_db_name}.result r
         JOIN ${stats_db_name}.project_results pr ON r.id = pr.result
         JOIN ${stats_db_name}.project_tmp p ON p.id = pr.id; /*EOS*/