------------------------------------------------------
------------------------------------------------------
-- Dataset table/view and Dataset related tables/views
------------------------------------------------------
------------------------------------------------------

-- Dataset temporary table supporting updates
CREATE TABLE ${stats_db_name}.dataset_tmp (id STRING, title STRING, publisher STRING, journal STRING, date STRING, year STRING, bestlicence STRING, embargo_end_date STRING, delayed BOOLEAN,   authors INT,   source STRING,   abstract BOOLEAN,   type STRING ) clustered by (id) into 100 buckets stored AS orc tblproperties('transactional'='true');

INSERT INTO ${stats_db_name}.dataset_tmp SELECT substr(d.id, 4) AS id, d.title[0].value AS title, d.publisher.value AS publisher, cast(null AS string) AS journal, 
d.dateofacceptance.value as date, date_format(d.dateofacceptance.value,'yyyy') AS year, d.bestaccessright.classname AS bestlicence,
d.embargoenddate.value AS embargo_end_date, false AS delayed, size(d.author) AS authors , concat_ws('\u003B',d.source.value) AS source,
 CASE WHEN SIZE(d.description) > 0 THEN TRUE ELSE FALSE end AS abstract,
'dataset' AS type
FROM ${openaire_db_name}.dataset d
WHERE d.datainfo.deletedbyinference=FALSE;

-- Dataset_citations
CREATE TABLE ${stats_db_name}.dataset_citations AS SELECT substr(d.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS result FROM ${openaire_db_name}.dataset d  LATERAL VIEW explode(d.extrainfo) citations AS citation WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="" and d.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.dataset_classifications AS SELECT substr(p.id, 4) AS id, instancetype.classname AS type FROM ${openaire_db_name}.dataset p LATERAL VIEW explode(p.instance.instancetype) instances AS instancetype where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.dataset_concepts AS SELECT substr(p.id, 4) as id, contexts.context.id as concept from ${openaire_db_name}.dataset p LATERAL VIEW explode(p.context) contexts as context where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.dataset_datasources AS SELECT p.id, case when d.id IS NULL THEN 'other' ELSE p.datasource END AS datasource FROM (SELECT  substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) AS datasource 
FROM ${openaire_db_name}.dataset p LATERAL VIEW explode(p.instance) instances AS instance where p.datainfo.deletedbyinference=false) p LEFT OUTER JOIN
(SELECT substr(d.id, 4) id FROM ${openaire_db_name}.datasource d WHERE d.datainfo.deletedbyinference=false) d ON p.datasource = d.id;

CREATE TABLE ${stats_db_name}.dataset_languages AS SELECT substr(p.id, 4) AS id, p.language.classname AS language FROM ${openaire_db_name}.dataset p where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.dataset_oids AS SELECT substr(p.id, 4) AS id, oids.ids AS oid FROM ${openaire_db_name}.dataset p LATERAL VIEW explode(p.originalid) oids AS ids where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.dataset_pids AS SELECT substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value AS pid FROM ${openaire_db_name}.dataset p LATERAL VIEW explode(p.pid) pids AS ppid where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.dataset_topics AS SELECT substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS type, subjects.subject.value AS topic FROM ${openaire_db_name}.dataset p LATERAL VIEW explode(p.subject) subjects AS subject where p.datainfo.deletedbyinference=false;

ANALYZE TABLE ${stats_db_name}.dataset_tmp COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_tmp COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_classifications COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_classifications COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_concepts COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_concepts COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_datasources COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_datasources COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_languages COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_languages COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_oids COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_oids COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_pids COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_pids COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.dataset_topics COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.dataset_topics COMPUTE STATISTICS FOR COLUMNS;