--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Otherresearchproduct table/view and Otherresearchproduct related tables/views
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- Otherresearchproduct temporary table supporting updates
CREATE TABLE ${stats_db_name}.otherresearchproduct_tmp
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
) CLUSTERED BY (id) INTO 100 buckets stored AS orc tblproperties ('transactional' = 'true');

INSERT INTO ${stats_db_name}.otherresearchproduct_tmp
SELECT substr(o.id, 4)                                            AS id,
       o.title[0].value                                           AS title,
       o.publisher.value                                          AS publisher,
       CAST(NULL AS string)                                       AS journal,
       o.dateofacceptance.value                                   AS DATE,
       date_format(o.dateofacceptance.value, 'yyyy')              AS year,
       o.bestaccessright.classname                                AS bestlicence,
       o.embargoenddate.value                                     as embargo_end_date,
       FALSE                                                      AS delayed,
       SIZE(o.author)                                             AS authors,
       concat_ws('\u003B', o.source.value)                        AS source,
       CASE WHEN SIZE(o.description) > 0 THEN TRUE ELSE FALSE END AS abstract,
       'other'                                                    AS type
FROM ${openaire_db_name}.otherresearchproduct o
WHERE o.datainfo.deletedbyinference = FALSE;

-- Otherresearchproduct_citations
CREATE TABLE ${stats_db_name}.otherresearchproduct_citations AS
SELECT substr(o.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS RESULT
FROM ${openaire_db_name}.otherresearchproduct o LATERAL VIEW explode(o.extrainfo) citations AS citation
WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") != ""
  and o.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.otherresearchproduct_classifications AS
SELECT substr(p.id, 4) AS id, instancetype.classname AS type
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.instance.instancetype) instances AS instancetype
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.otherresearchproduct_concepts AS
SELECT substr(p.id, 4) as id, case
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+::.+$' then contexts.context.id
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+$' then concat(contexts.context.id, '::other')
                                  when contexts.context.id RLIKE '^[^::]+$' then concat(contexts.context.id, '::other::other') END as concept
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.context) contexts AS context
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.otherresearchproduct_datasources AS
SELECT p.id, CASE WHEN d.id IS NULL THEN 'other' ELSE p.datasource END AS datasource
FROM (SELECT substr(p.id, 4) AS id, substr(instances.instance.hostedby.key, 4) AS datasource
      from ${openaire_db_name}.otherresearchproduct p lateral view explode(p.instance) instances as instance
      where p.datainfo.deletedbyinference = false) p
         LEFT OUTER JOIN(SELECT substr(d.id, 4) id
                         from ${openaire_db_name}.datasource d
                         WHERE d.datainfo.deletedbyinference = false) d on p.datasource = d.id;

CREATE TABLE ${stats_db_name}.otherresearchproduct_languages AS
SELECT substr(p.id, 4) AS id, p.language.classname AS language
FROM ${openaire_db_name}.otherresearchproduct p
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.otherresearchproduct_oids AS
SELECT substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.otherresearchproduct_pids AS
SELECT substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value AS pid
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.pid) pids AS ppid
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.otherresearchproduct_topics AS
SELECT substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS type, subjects.subject.value AS topic
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.subject) subjects AS subject
where p.datainfo.deletedbyinference = false;

-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_tmp COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_tmp COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_classifications COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_classifications COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_concepts COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_concepts COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_datasources COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_datasources COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_languages COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_languages COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_oids COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_oids COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_pids COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_pids COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_topics COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct_topics COMPUTE STATISTICS FOR COLUMNS;