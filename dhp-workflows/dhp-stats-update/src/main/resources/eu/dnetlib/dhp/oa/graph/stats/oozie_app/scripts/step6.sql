set mapred.job.queue.name=analytics; /*EOS*/

------------------------------------------------------
------------------------------------------------------
-- Project table/view and Project related tables/views
------------------------------------------------------
------------------------------------------------------
DROP TABLE IF EXISTS ${stats_db_name}.project_oids purge; /*EOS*/

CREATE TABLE ${stats_db_name}.project_oids STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.project p LATERAL VIEW explode(p.originalid) oids AS ids
where p.datainfo.deletedbyinference=false  and p.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.project_organizations purge; /*EOS*/

CREATE TABLE ${stats_db_name}.project_organizations STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(r.source, 4) AS id, substr(r.target, 4) AS organization
from ${openaire_db_name}.relation r
WHERE r.reltype = 'projectOrganization' and r.source like '40|%'
  and r.datainfo.deletedbyinference = false and r.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.project_results purge; /*EOS*/

CREATE TABLE ${stats_db_name}.project_results STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ substr(r.target, 4) AS id, substr(r.source, 4) AS result, r.datainfo.provenanceaction.classname as provenance
FROM ${openaire_db_name}.relation r
WHERE r.reltype = 'resultProject' and r.target like '40|%'
  and r.datainfo.deletedbyinference = false and r.datainfo.invisible=false; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.project_classification purge; /*EOS*/

create table ${stats_db_name}.project_classification STORED AS PARQUET as
select /*+ COALESCE(100) */ substr(p.id, 4) as id, class.h2020programme.code, class.level1, class.level2, class.level3
from ${openaire_db_name}.project p
    lateral view explode(p.h2020classification) classifs as class
where p.datainfo.deletedbyinference=false and p.datainfo.invisible=false and class.h2020programme is not null; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.project purge; /*EOS*/

CREATE TABLE ${stats_db_name}.project stored as parquet as
with pr_pub as (
    select pr.id as pr_id, pub.id as pub_id,
        (case when datediff(pub.dt_dateofacceptance, pr.dt_enddate) > 0 then true else false end) as delayed,
        max(datediff(pub.dt_dateofacceptance, pr.dt_enddate)) as daysForlastPub
    from (select id, to_date(dateofacceptance.value) as dt_dateofacceptance from ${openaire_db_name}.publication
        where datainfo.deletedbyinference = false and datainfo.invisible = false) pub
    join ${openaire_db_name}.relation rel
        on rel.reltype = 'resultProject' and rel.relclass = 'isProducedBy' and rel.source=pub.id
            and rel.datainfo.deletedbyinference = false and rel.datainfo.invisible = false
    join (select id, to_date(enddate.value) as dt_enddate from ${openaire_db_name}.project
            where datainfo.deletedbyinference = false and datainfo.invisible = false) pr
        on pr.id=rel.target
    group by pr.id, pub.id, pub.dt_dateofacceptance, pr.dt_enddate
),
num_pubs_pr as (
    select pr_id, count( distinct pub_id) as num_pubs
    from pr_pub
    group by pr_id
),
pub_delayed as (
    select pr_id, pub_id, max(delayed) as delayed
    from pr_pub
    group by pr_id, pub_id
),
num_pub_delayed as (
    select pr_id, count(distinct pub_id) as num_delayed
    from pub_delayed
    where delayed
    group by pr_id
)
select /*+ COALESCE(100) */
    substr(p.id, 4)                                                             as id,
    p.acronym.value                                                             as acronym,
    p.title.value                                                               as title,
    xpath_string(p.fundingtree[0].value, '//funder/name')                       as funder,
    xpath_string(p.fundingtree[0].value, '//funding_level_0/name')              as funding_lvl0,
    xpath_string(p.fundingtree[0].value, '//funding_level_1/name')              as funding_lvl1,
    xpath_string(p.fundingtree[0].value, '//funding_level_2/name')              as funding_lvl2,
    p.ecsc39.value                                                              as ec39,
    p.contracttype.classname                                                    as type,
    p.startdate.value                                                           as startdate,
    p.enddate.value                                                             as enddate,
    year(p.startdate.value)                                                     as start_year,
    year(p.enddate.value)                                                       as end_year,
    cast(months_between(p.enddate.value, p.startdate.value) as int)             as duration,
    case when pr_pub.pub_id is null then 'no' else 'yes' end                    as haspubs,
    num_pubs_pr.num_pubs                                                        as numpubs,
    pr_pub.daysForlastPub                                                       as daysForlastPub,
    npd.num_delayed                                                             as delayedpubs,
    p.callidentifier.value                                                      as callidentifier,
    p.code.value                                                                as code,
    p.totalcost                                                                 as totalcost,
    p.fundedamount                                                              as fundedamount,
    p.currency.value                                                            as currency
from ${openaire_db_name}.project p
left outer join pr_pub on pr_pub.pr_id = p.id
left outer join num_pubs_pr on num_pubs_pr.pr_id = p.id
left outer join num_pub_delayed npd on npd.pr_id=p.id
where p.datainfo.deletedbyinference = false and p.datainfo.invisible = false; /*EOS*/


DROP TABLE IF EXISTS ${stats_db_name}.funder purge; /*EOS*/

create table ${stats_db_name}.funder STORED AS PARQUET as
select /*+ COALESCE(100) */ distinct xpath_string(fund, '//funder/id')        as id,
                xpath_string(fund, '//funder/name')      as name,
                xpath_string(fund, '//funder/shortname') as shortname,
                xpath_string(fundingtree[0].value, '//funder/jurisdiction') as country
from ${openaire_db_name}.project p lateral view explode(p.fundingtree.value) fundingtree as fund; /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.project_organization_contribution purge; /*EOS*/

CREATE TABLE ${stats_db_name}.project_organization_contribution STORED AS PARQUET AS
SELECT /*+ COALESCE(100) */ distinct substr(r.source, 4) AS project, substr(r.target, 4) AS organization,
properties[0].value contribution, properties[1].value currency
from ${openaire_db_name}.relation r
LATERAL VIEW explode (r.properties) properties
where properties[0].key='contribution' and r.reltype = 'projectOrganization' and r.source like '40|%'
and properties[0].value>0.0 and r.datainfo.deletedbyinference = false and r.datainfo.invisible=false; /*EOS*/