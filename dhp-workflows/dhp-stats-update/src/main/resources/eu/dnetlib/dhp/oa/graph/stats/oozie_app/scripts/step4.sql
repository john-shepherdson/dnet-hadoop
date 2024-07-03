--------------------------------------------------------
--------------------------------------------------------
-- Software table/view and Software related tables/views
--------------------------------------------------------
--------------------------------------------------------

-- Software temporary table supporting updates
DROP TABLE IF EXISTS ${stats_db_name}.software purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software stored as parquet as
with soft_pr as (
    select soft.id as soft_id, case when (to_date(soft.dateofacceptance.value) > to_date( pj.enddate.value)) then true else false end as delayed
    from ${openaire_db_name}.software soft
        join ${openaire_db_name}.relation rel
            on reltype = 'resultProject' and relclass = 'isProducedBy' and rel.source=soft.id
                and rel.datainfo.deletedbyinference = false and rel.datainfo.invisible = false
        join ${openaire_db_name}.project pj on pj.id=rel.target and pj.datainfo.deletedbyinference = false and pj.datainfo.invisible = false
    where soft.datainfo.deletedbyinference = false and soft.datainfo.invisible = false
),
soft_delayed as (
    select soft_id, max(delayed) as delayed
    from soft_pr
    group by soft_id
)
select /*+ COALESCE(100) */
    substr(soft.id, 4)                                                       as id,
    soft.title[0].value                                                      as title,
    soft.publisher.value                                                     as publisher,
    cast(null as string)                                                     as journal,
    soft.dateofacceptance.value                                              as date,
    date_format(soft.dateofacceptance.value, 'yyyy')                         as year,
    soft.bestaccessright.classname                                           as bestlicence,
    soft.embargoenddate.value                                                as embargo_end_date,
    coalesce(soft_delayed.delayed, false)                                    as delayed, -- It's delayed, when the software was published after the end of the project.
    size(soft.author)                                                        as authors,
    concat_ws('\u003B', soft.source.value)                                   as source,
    case when size(soft.description) > 0 then true else false end            as abstract,
    'software'                                                               as type
from ${openaire_db_name}.software soft
         left outer join soft_delayed on soft.id=soft_delayed.soft_id
where soft.datainfo.deletedbyinference = false and soft.datainfo.invisible = false; /*EOS*/


DROP TABLE IF EXISTS ${stats_db_name}.software_citations purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_citations STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(s.id, 4) as id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS cites
FROM ${openaire_db_name}.software s
         LATERAL VIEW explode(s.extrainfo) citations as citation
where xpath_string(citation.value, "//citation/id[@type='openaire']/@value") != ""
  and s.datainfo.deletedbyinference = false and s.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_classifications purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_classifications STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, instancetype.classname AS type
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.instance.instancetype) instances AS instancetype
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_concepts purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_concepts STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) as id, case
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+::.+$' then contexts.context.id
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+$' then concat(contexts.context.id, '::other')
                                  when contexts.context.id RLIKE '^[^::]+$' then concat(contexts.context.id, '::other::other') END as concept
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.context) contexts AS context
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_datasources purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_datasources STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ p.id, CASE WHEN d.id IS NULL THEN 'other' ELSE p.datasource end as datasource
FROM (
         SELECT substr(p.id, 4) AS id, substr(instances.instance.hostedby.key, 4) AS datasource
         FROM ${openaire_db_name}.software p
                  LATERAL VIEW explode(p.instance) instances AS instance
         where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false) p
         LEFT OUTER JOIN (
    SELECT substr(d.id, 4) id
    FROM ${openaire_db_name}.datasource d
    WHERE d.datainfo.deletedbyinference = false and d.datainfo.invisible=false) d ON p.datasource = d.id; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_languages purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_languages STORED AS PARQUET AS
select /*+ COALESCE(100) */ substr(p.id, 4) AS id, p.language.classname AS language
FROM ${openaire_db_name}.software p
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_oids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_oids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_pids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_pids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value AS pid
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.pid) pids AS ppid
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.software_topics purge; /*EOS*/

CREATE TABLE ${stats_db_name}.software_topics STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS type, subjects.subject.value AS topic
FROM ${openaire_db_name}.software p
         LATERAL VIEW explode(p.subject) subjects AS subject
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/