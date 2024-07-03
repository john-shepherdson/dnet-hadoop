-- noinspection SqlNoDataSourceInspectionForFile

------------------------------------------------------------
------------------------------------------------------------
-- Datasource table/view and Datasource related tables/views
------------------------------------------------------------
------------------------------------------------------------
DROP TABLE IF EXISTS ${stats_db_name}.datasource purge; /*EOS*/

CREATE TABLE ${stats_db_name}.datasource stored as parquet as
with piwik_datasource as (
    select id, split(originalidd, '\\:')[1] as piwik_id
    from ${openaire_db_name}.datasource
             lateral view explode(originalid) temp as originalidd
    where originalidd like "piwik:%"
)
select /*+ COALESCE(100) */
       substr(dtrce.id, 4)                                                                                                 as id,
       case when dtrce.officialname.value='Unknown Repository' then 'Other' else dtrce.officialname.value end              as name,
       dtrce.datasourcetype.classname                                                                                      as type,
       dtrce.dateofvalidation.value                                                                                        as dateofvalidation,
       case when dtrce.dateofvalidation.value='-1' then null else date_format(dtrce.dateofvalidation.value, 'yyyy') end    as yearofvalidation,
       case when res.d_id is null then false else true end                                                                 as harvested,
       case when piwik_d.piwik_id is null then 0 else piwik_d.piwik_id end                                                 as piwik_id,
       dtrce.latitude.value                                                                                                as latitude,
       dtrce.longitude.value                                                                                               as longitude,
       dtrce.websiteurl.value                                                                                              as websiteurl,
       dtrce.openairecompatibility.classid                                                                                 as compatibility,
       dtrce.journal.issnprinted                                                                                           as issn_printed,
       dtrce.journal.issnonline                                                                                            as issn_online
from ${openaire_db_name}.datasource dtrce
         left outer join (select inst.hostedby.key as d_id from ${openaire_db_name}.result lateral view outer explode (instance) insts as inst) res on res.d_id=dtrce.id
         left outer join piwik_datasource piwik_d on piwik_d.id=dtrce.id
where dtrce.datainfo.deletedbyinference = false and dtrce.datainfo.invisible = false; /*EOS*/


DROP TABLE IF EXISTS ${stats_db_name}.datasource_languages purge; /*EOS*/

CREATE TABLE ${stats_db_name}.datasource_languages STORED AS PARQUET AS
SELECT substr(d.id, 4) AS id, langs.languages AS language
FROM ${openaire_db_name}.datasource d LATERAL VIEW explode(d.odlanguages.value) langs AS languages
where d.datainfo.deletedbyinference=false and d.datainfo.invisible=false; -- /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.datasource_oids purge; -- /*EOS*/

CREATE TABLE ${stats_db_name}.datasource_oids STORED AS PARQUET AS
SELECT substr(d.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.datasource d LATERAL VIEW explode(d.originalid) oids AS ids
where d.datainfo.deletedbyinference=false and d.datainfo.invisible=false; -- /*EOS*/

DROP TABLE IF EXISTS ${stats_db_name}.datasource_organizations purge; -- /*EOS*/

CREATE TABLE ${stats_db_name}.datasource_organizations STORED AS PARQUET AS
SELECT substr(r.target, 4) AS id, substr(r.source, 4) AS organization
FROM ${openaire_db_name}.relation r
WHERE r.reltype = 'datasourceOrganization' and r.datainfo.deletedbyinference = false and r.source like '20|%' and r.datainfo.invisible=false; -- /*EOS*/

-- datasource sources:
-- where the datasource info have been collected from.
DROP TABLE IF EXISTS ${stats_db_name}.datasource_sources purge; -- /*EOS*/

create table if not exists ${stats_db_name}.datasource_sources STORED AS PARQUET AS
select substr(d.id, 4) as id, substr(cf.key, 4) as datasource
from ${openaire_db_name}.datasource d lateral view explode(d.collectedfrom) cfrom as cf
where d.datainfo.deletedbyinference = false and d.datainfo.invisible=false; -- /*EOS*/

CREATE OR REPLACE VIEW ${stats_db_name}.datasource_results AS
SELECT datasource AS id, id AS result
FROM ${stats_db_name}.result_datasources; -- /*EOS*/
