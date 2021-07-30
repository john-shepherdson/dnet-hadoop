--------------------------------------------------------
--------------------------------------------------------
-- Software table/view and Software related tables/views
--------------------------------------------------------
--------------------------------------------------------

-- Software temporary table supporting updates
CREATE TABLE ${stats_db_name}.software_tmp
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
    clustered by (id) INTO 100 buckets stored AS orc tblproperties ('transactional' = 'true');

INSERT INTO ${stats_db_name}.software_tmp
SELECT substr(s.id, 4)                                            as id,
       s.title[0].value                                           AS title,
       s.publisher.value                                          AS publisher,
       CAST(NULL AS string)                                       AS journal,
       s.dateofacceptance.value                                   AS DATE,
       date_format(s.dateofacceptance.value, 'yyyy')              AS YEAR,
       s.bestaccessright.classname                                AS bestlicence,
       s.embargoenddate.value                                     AS embargo_end_date,
       FALSE                                                      AS delayed,
       SIZE(s.author)                                             AS authors,
       concat_ws('\u003B', s.source.value)                        AS source,
       CASE WHEN SIZE(s.description) > 0 THEN TRUE ELSE FALSE END AS abstract,
       'software'                                                 as type
from ${openaire_db_name}.software s
where s.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.software_citations AS
SELECT substr(s.id, 4) as id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS cites
FROM ${openaire_db_name}.software s
         LATERAL VIEW explode(s.extrainfo) citations as citation
where xpath_string(citation.value, "//citation/id[@type='openaire']/@value") != ""
  and s.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.software_classifications AS
SELECT substr(p.id, 4) AS id, instancetype.classname AS type
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.instance.instancetype) instances AS instancetype
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.software_concepts AS
SELECT substr(p.id, 4) as id, case
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+::.+$' then contexts.context.id
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+$' then concat(contexts.context.id, '::other')
                                  when contexts.context.id RLIKE '^[^::]+$' then concat(contexts.context.id, '::other::other') END as concept
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.context) contexts AS context
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.software_datasources AS
SELECT p.id, CASE WHEN d.id IS NULL THEN 'other' ELSE p.datasource end as datasource
FROM (
         SELECT substr(p.id, 4) AS id, substr(instances.instance.hostedby.key, 4) AS datasource
         FROM ${openaire_db_name}.software p
                  LATERAL VIEW explode(p.instance) instances AS instance
         where p.datainfo.deletedbyinference = false) p
         LEFT OUTER JOIN (
    SELECT substr(d.id, 4) id
    FROM ${openaire_db_name}.datasource d
    WHERE d.datainfo.deletedbyinference = false) d ON p.datasource = d.id;

CREATE TABLE ${stats_db_name}.software_languages AS
select substr(p.id, 4) AS id, p.language.classname AS language
FROM ${openaire_db_name}.software p
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.software_oids AS
SELECT substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.software_pids AS
SELECT substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value AS pid
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.pid) pids AS ppid
where p.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.software_topics AS
SELECT substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS type, subjects.subject.value AS topic
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.subject) subjects AS subject
where p.datainfo.deletedbyinference = false;