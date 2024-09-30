set mapred.job.queue.name=analytics; /*EOS*/

------------------------------------------------------
------------------------------------------------------
-- Dataset table/view and Dataset related tables/views
------------------------------------------------------
------------------------------------------------------

-- Dataset temporary table supporting updates
DROP TABLE IF EXISTS ${stats_db_name}.dataset purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset stored as parquet as
with datast_pr as (
    select datast.id as datast_id, case when (to_date(datast.dateofacceptance.value) > to_date( pj.enddate.value)) then true else false end as delayed
    from ${openaire_db_name}.dataset datast
        join ${openaire_db_name}.relation rel
            on reltype = 'resultProject' and relclass = 'isProducedBy' and rel.source=datast.id
                and rel.datainfo.deletedbyinference = false and rel.datainfo.invisible = false
    join ${openaire_db_name}.project pj on pj.id=rel.target and pj.datainfo.deletedbyinference = false and pj.datainfo.invisible = false
    where datast.datainfo.deletedbyinference = false and datast.datainfo.invisible = false
),
datast_delayed as (
    select datast_id, max(delayed) as delayed
    from datast_pr
    group by datast_id
)
select /*+ COALESCE(100) */
    substr(datast.id, 4)                                                  as id,
    datast.title[0].value                                                 as title,
    datast.publisher.value                                                as publisher,
    cast(null as string)                                                  as journal,
    datast.dateofacceptance.value                                         as date,
    date_format(datast.dateofacceptance.value, 'yyyy')                    as year,
    datast.bestaccessright.classname                                      as bestlicence,
    datast.embargoenddate.value                                           as embargo_end_date,
    coalesce(datast_delayed.delayed, false)                               as delayed, -- It's delayed, when the dataset was published after the end of the project.
    size(datast.author)                                                   as authors,
    concat_ws('\u003B', datast.source.value)                              as source,
    case when size(datast.description) > 0 then true else false end       as abstract,
    'dataset'                                                             as type
from ${openaire_db_name}.dataset datast
    left outer join datast_delayed on datast.id=datast_delayed.datast_id
where datast.datainfo.deletedbyinference = false and datast.datainfo.invisible = false; /*EOS*/


DROP TABLE IF EXISTS ${stats_db_name}.dataset_citations purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_citations STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(d.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS cites
FROM ${openaire_db_name}.dataset d
         LATERAL VIEW explode(d.extrainfo) citations AS citation
WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") != ""
  and d.datainfo.deletedbyinference = false and d.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_classifications purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_classifications STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, instancetype.classname AS type
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.instance.instancetype) instances AS instancetype
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_concepts purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_concepts STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) as id, case
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+::.+$' then contexts.context.id
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+$' then concat(contexts.context.id, '::other')
                                  when contexts.context.id RLIKE '^[^::]+$' then concat(contexts.context.id, '::other::other') END as concept
from ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.context) contexts as context
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_datasources purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_datasources STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ p.id, case when d.id IS NULL THEN 'other' ELSE p.datasource END AS datasource
FROM (
         SELECT substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) AS datasource
         FROM ${openaire_db_name}.dataset p
                  LATERAL VIEW explode(p.instance) instances AS instance
         where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false) p
         LEFT OUTER JOIN (
    SELECT substr(d.id, 4) id
    FROM ${openaire_db_name}.datasource d
    WHERE d.datainfo.deletedbyinference = false and d.datainfo.invisible=false) d ON p.datasource = d.id; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_languages purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_languages STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, p.language.classname AS language
FROM ${openaire_db_name}.dataset p
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_oids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_oids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_pids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_pids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value AS pid
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.pid) pids AS ppid
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.dataset_topics purge; /*EOS*/

CREATE TABLE ${stats_db_name}.dataset_topics STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS type, subjects.subject.value AS topic
FROM ${openaire_db_name}.dataset p
         LATERAL VIEW explode(p.subject) subjects AS subject
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/