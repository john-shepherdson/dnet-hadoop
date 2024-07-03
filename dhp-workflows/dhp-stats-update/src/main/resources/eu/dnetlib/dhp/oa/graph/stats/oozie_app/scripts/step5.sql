--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Otherresearchproduct table/view and Otherresearchproduct related tables/views
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- Otherresearchproduct temporary table supporting updates
DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct stored as parquet as
with other_pr as (
    select other.id as other_id, case when (to_date(other.dateofacceptance.value) > to_date( pj.enddate.value)) then true else false end as delayed
    from ${openaire_db_name}.otherresearchproduct other
    join ${openaire_db_name}.relation rel
        on reltype = 'resultProject' and relclass = 'isProducedBy' and rel.source=other.id
            and rel.datainfo.deletedbyinference = false and rel.datainfo.invisible = false
    join ${openaire_db_name}.project pj on pj.id=rel.target and pj.datainfo.deletedbyinference = false and pj.datainfo.invisible = false
    where other.datainfo.deletedbyinference = false and other.datainfo.invisible = false
),
other_delayed as (
    select other_id, max(delayed) as delayed
    from other_pr
    group by other_id
)
select /*+ COALESCE(100) */
    substr(other.id, 4)                                            as id,
    other.title[0].value                                           as title,
    other.publisher.value                                          as publisher,
    cast(null as string)                                           as journal,
    other.dateofacceptance.value                                   as date,
    date_format(other.dateofacceptance.value, 'yyyy')              as year,
    other.bestaccessright.classname                                as bestlicence,
    other.embargoenddate.value                                     as embargo_end_date,
    false                                                          as delayed,
    size(other.author)                                             as authors,
    concat_ws('\u003B', other.source.value)                        as source,
    case when size(other.description) > 0 then true else false end as abstract,
    'other'                                                        as type
from ${openaire_db_name}.otherresearchproduct other
    left outer join other_delayed on other.id=other_delayed.other_id
where other.datainfo.deletedbyinference = false and other.datainfo.invisible = false; /*EOS*/


-- Otherresearchproduct_citations
DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_citations purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_citations STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(o.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS cites
FROM ${openaire_db_name}.otherresearchproduct o LATERAL VIEW explode(o.extrainfo) citations AS citation
WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") != ""
  and o.datainfo.deletedbyinference = false and o.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_classifications purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_classifications STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, instancetype.classname AS type
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.instance.instancetype) instances AS instancetype
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_concepts purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_concepts STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) as id, case
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+::.+$' then contexts.context.id
                                  when contexts.context.id RLIKE '^[^::]+::[^::]+$' then concat(contexts.context.id, '::other')
                                  when contexts.context.id RLIKE '^[^::]+$' then concat(contexts.context.id, '::other::other') END as concept
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.context) contexts AS context
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_datasources purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_datasources STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ p.id, CASE WHEN d.id IS NULL THEN 'other' ELSE p.datasource END AS datasource
FROM (SELECT substr(p.id, 4) AS id, substr(instances.instance.hostedby.key, 4) AS datasource
      from ${openaire_db_name}.otherresearchproduct p lateral view explode(p.instance) instances as instance
      where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false) p
         LEFT OUTER JOIN (SELECT substr(d.id, 4) id
                         from ${openaire_db_name}.datasource d
                         WHERE d.datainfo.deletedbyinference = false and d.datainfo.invisible=false) d on p.datasource = d.id; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_languages purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_languages STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, p.language.classname AS language
FROM ${openaire_db_name}.otherresearchproduct p
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_oids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_oids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_pids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_pids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value AS pid
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.pid) pids AS ppid
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_topics purge; /*EOS*/

CREATE TABLE ${stats_db_name}.otherresearchproduct_topics STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS type, subjects.subject.value AS topic
FROM ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.subject) subjects AS subject
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/