------------------------------------------------------
------------------------------------------------------
-- Dataset table/view and Dataset related tables/views
------------------------------------------------------
------------------------------------------------------

-- Dataset temporary table supporting updates
DROP TABLE IF EXISTS ${stats_db_name}.dataset_tmp purge;

CREATE TABLE ${stats_db_name}.dataset_tmp
(
    id               STRING,
    title            STRING,
    publisher        STRING,
    journal          STRING,
    date             STRING,
    year             STRING,
    bestlicence      STRING,
    embargo_end_date STRING,
    delayed          BOOLEAN,
    authors          INT,
    source           STRING,
    abstract         BOOLEAN,
    type             STRING
)
    clustered by (id) into 100 buckets stored AS orc tblproperties ('transactional' = 'true');

INSERT INTO ${stats_db_name}.dataset_tmp
SELECT substr(d.id, 4)                                            AS id,
       d.title[0].value                                           AS title,
       d.publisher.value                                          AS publisher,
       cast(null AS string)                                       AS journal,
       d.dateofacceptance.value                                   as date,
       date_format(d.dateofacceptance.value, 'yyyy')              AS year,
       d.bestaccessright.classname                                AS bestlicence,
       d.embargoenddate.value                                     AS embargo_end_date,
       false                                                      AS delayed,
       size(d.author)                                             AS authors,
       concat_ws('\u003B', d.source.value)                        AS source,
       CASE WHEN SIZE(d.description) > 0 THEN TRUE ELSE FALSE end AS abstract,
       'dataset'                                                  AS type
FROM ${openaire_db_name}.dataset d
WHERE d.datainfo.deletedbyinference = FALSE and d.datainfo.invisible=false;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_citations purge;

CREATE TABLE ${stats_db_name}.dataset_citations STORED AS PARQUET AS
SELECT substr(d.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS cites
FROM ${openaire_db_name}.dataset d
         LATERAL VIEW explode(d.extrainfo) citations AS citation
WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") != ""
  and d.datainfo.deletedbyinference = false and d.datainfo.invisible=false;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_classifications purge;

CREATE TABLE ${stats_db_name}.dataset_classifications STORED AS PARQUET AS
SELECT substr(p.id, 4) AS id, instancetype.classname AS type
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.instance.instancetype) instances AS instancetype
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_concepts purge;

CREATE TABLE ${stats_db_name}.dataset_concepts STORED AS PARQUET AS
SELECT substr(p.id, 4) as id, case
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+::.+$' then contexts.context.id
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+$' then concat(contexts.context.id, '::other')
                                  when contexts.context.id RLIKE '^[^::]+$' then concat(contexts.context.id, '::other::other') END as concept
from ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.context) contexts as context
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_datasources purge;

CREATE TABLE ${stats_db_name}.dataset_datasources STORED AS PARQUET AS
SELECT p.id, case when d.id IS NULL THEN 'other' ELSE p.datasource END AS datasource
FROM (
         SELECT substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) AS datasource
         FROM ${openaire_db_name}.dataset p
                  LATERAL VIEW explode(p.instance) instances AS instance
         where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false) p
         LEFT OUTER JOIN (
    SELECT substr(d.id, 4) id
    FROM ${openaire_db_name}.datasource d
    WHERE d.datainfo.deletedbyinference = false and d.datainfo.invisible=false) d ON p.datasource = d.id;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_languages purge;

CREATE TABLE ${stats_db_name}.dataset_languages STORED AS PARQUET AS
SELECT substr(p.id, 4) AS id, p.language.classname AS language
FROM ${openaire_db_name}.dataset p
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_oids purge;

CREATE TABLE ${stats_db_name}.dataset_oids STORED AS PARQUET AS
SELECT substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_pids purge;

CREATE TABLE ${stats_db_name}.dataset_pids STORED AS PARQUET AS
SELECT substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value AS pid
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.pid) pids AS ppid
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false;

DROP TABLE IF EXISTS ${stats_db_name}.dataset_topics purge;

CREATE TABLE ${stats_db_name}.dataset_topics STORED AS PARQUET AS
SELECT substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS type, subjects.subject.value AS topic
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.subject) subjects AS subject
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false;