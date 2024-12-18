set mapred.job.queue.name=analytics; /*EOS*/

--------------------------------------------------------------
--------------------------------------------------------------
-- Publication table/view and Publication related tables/views
--------------------------------------------------------------
--------------------------------------------------------------

-- Publication temporary table
DROP TABLE IF EXISTS ${stats_db_name}.publication purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication stored as parquet as
with pub_pr as (
    select pub.id as pub_id, case when (to_date(pub.dateofacceptance.value) > to_date( pj.enddate.value)) then true else false end as delayed
    from ${openaire_db_name}.publication pub
             join ${openaire_db_name}.relation rel
                  on reltype = 'resultProject' and relclass = 'isProducedBy' and rel.source=pub.id
                      and rel.datainfo.deletedbyinference = false and rel.datainfo.invisible = false
             join ${openaire_db_name}.project pj on pj.id=rel.target and pj.datainfo.deletedbyinference = false and pj.datainfo.invisible = false
    where pub.datainfo.deletedbyinference = false and pub.datainfo.invisible = false
),
 pub_delayed as (
     select pub_id, max(delayed) as delayed
     from pub_pr
     group by pub_id
 )
select /*+ COALESCE(100) */
    substr(pub.id, 4)                                                     as id,
    pub.title[0].value                                                    as title,
    pub.publisher.value                                                   as publisher,
    pub.journal.name                                                      as journal,
    pub.dateofacceptance.value                                            as date,
    date_format(pub.dateofacceptance.value, 'yyyy')                       as year,
    pub.bestaccessright.classname                                         as bestlicence,
    pub.embargoenddate.value                                              as embargo_end_date,
    coalesce(pub_delayed.delayed, false)                                  as delayed, -- It's delayed, when the publication was published after the end of at least one of its projects.
    size(pub.author)                                                      as authors,
    concat_ws('\u003B', pub.source.value)                                 as source,
    case when size(pub.description) > 0 then true else false end          as abstract,
    'publication'                                                         as type
from ${openaire_db_name}.publication pub
    left outer join pub_delayed on pub.id=pub_delayed.pub_id
where pub.datainfo.deletedbyinference = false and pub.datainfo.invisible = false; /*EOS*/


DROP TABLE IF EXISTS ${stats_db_name}.publication_classifications purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_classifications STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) as id, instancetype.classname as type
from ${openaire_db_name}.publication p
         LATERAL VIEW explode(p.instance.instancetype) instances as instancetype
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.publication_concepts purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_concepts STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) as id, case
    when contexts.context.id RLIKE '^[^::]+::[^::]+::.+$' then contexts.context.id
    when contexts.context.id RLIKE '^[^::]+::[^::]+$' then concat(contexts.context.id, '::other')
    when contexts.context.id RLIKE '^[^::]+$' then concat(contexts.context.id, '::other::other') END as concept
from ${openaire_db_name}.publication p
         LATERAL VIEW explode(p.context) contexts as context
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.publication_datasources purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_datasources STORED AS PARQUET as
SELECT /*+ COALESCE(100) */ p.id, case when d.id is null then 'other' else p.datasource end as datasource
FROM (
         SELECT substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) as datasource
         from ${openaire_db_name}.publication p lateral view explode(p.instance) instances as instance
         where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false) p
         LEFT OUTER JOIN (
    SELECT substr(d.id, 4) id
    from ${openaire_db_name}.datasource d
    WHERE d.datainfo.deletedbyinference = false and d.datainfo.invisible=false) d on p.datasource = d.id; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.publication_languages purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_languages STORED AS PARQUET AS
select /*+ COALESCE(100) */ substr(p.id, 4) as id, p.language.classname as language
FROM ${openaire_db_name}.publication p
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.publication_oids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_oids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.publication p
         LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.publication_pids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_pids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value as pid
FROM ${openaire_db_name}.publication p
         LATERAL VIEW explode(p.pid) pids AS ppid
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.publication_topics purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_topics STORED AS PARQUET as
select /*+ COALESCE(100) */ substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS TYPE, subjects.subject.value AS topic
FROM ${openaire_db_name}.publication p
         LATERAL VIEW explode(p.subject) subjects AS subject
where p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.publication_citations purge; /*EOS*/

CREATE TABLE ${stats_db_name}.publication_citations STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS cites
FROM ${openaire_db_name}.publication p
         lateral view explode(p.extrainfo) citations AS citation
WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") != ""
  and p.datainfo.deletedbyinference = false and p.datainfo.invisible=false; /*EOS*/
