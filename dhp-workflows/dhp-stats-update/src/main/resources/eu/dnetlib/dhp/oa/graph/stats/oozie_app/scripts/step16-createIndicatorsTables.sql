-- Sprint 1 ----
drop table if exists ${stats_db_name}.indi_pub_green_oa purge;

create table if not exists ${stats_db_name}.indi_pub_green_oa stored as parquet as
select distinct p.id, coalesce(green_oa, 0) as green_oa
from ${stats_db_name}.publication p
         left outer join (
    select p.id, 1 as green_oa
    from ${stats_db_name}.publication p
             join ${stats_db_name}.result_instance ri on ri.id = p.id
             join ${stats_db_name}.datasource on datasource.id = ri.hostedby
    where datasource.type like '%Repository%'
      and (ri.accessright = 'Open Access'
        or ri.accessright = 'Embargo' or ri.accessright = 'Open Source')) tmp
                         on p.id= tmp.id;

drop table if exists ${stats_db_name}.indi_pub_grey_lit purge;

create table if not exists ${stats_db_name}.indi_pub_grey_lit stored as parquet as
select distinct p.id, coalesce(grey_lit, 0) as grey_lit
from ${stats_db_name}.publication p
         left outer join (
    select p.id, 1 as grey_lit
    from ${stats_db_name}.publication p
             join ${stats_db_name}.result_classifications rt on rt.id = p.id
    where rt.type not in ('Article','Part of book or chapter of book','Book','Doctoral thesis','Master thesis','Data Paper', 'Thesis', 'Bachelor thesis', 'Conference object') and
        not exists (select 1 from ${stats_db_name}.result_classifications rc where type ='Other literature type'
                                                              and rc.id=p.id)) tmp on p.id=tmp.id;

drop table if exists ${stats_db_name}.indi_pub_doi_from_crossref purge;

create table if not exists ${stats_db_name}.indi_pub_doi_from_crossref stored as parquet as
select distinct p.id, coalesce(doi_from_crossref, 0) as doi_from_crossref
from ${stats_db_name}.publication p
         left outer join
     (select ri.id, 1 as doi_from_crossref from ${stats_db_name}.result_instance ri
                                                    join ${stats_db_name}.datasource d on d.id = ri.collectedfrom
      where pidtype='Digital Object Identifier' and d.name ='Crossref') tmp
     on tmp.id=p.id;

-- Sprint 2 ----
drop table if exists ${stats_db_name}.indi_result_has_cc_licence purge;

create table if not exists ${stats_db_name}.indi_result_has_cc_licence stored as parquet as
select distinct r.id, (case when lic='' or lic is null then 0 else 1 end) as has_cc_license
from ${stats_db_name}.result r
left outer join (select r.id, license.type as lic from ${stats_db_name}.result r
                                                                    join ${stats_db_name}.result_licenses as license on license.id = r.id
                          where lower(license.type) LIKE '%creativecommons.org%' OR lower(license.type) LIKE '%cc-%') tmp
                         on r.id= tmp.id;

drop table if exists ${stats_db_name}.indi_result_has_cc_licence_url purge;

create table if not exists ${stats_db_name}.indi_result_has_cc_licence_url stored as parquet as
select distinct r.id, case when lic_host='' or lic_host is null then 0 else 1 end as has_cc_license_url
from ${stats_db_name}.result r
         left outer join (select r.id, lower(parse_url(license.type, "HOST")) as lic_host
                          from ${stats_db_name}.result r
                                   join ${stats_db_name}.result_licenses as license on license.id = r.id
                          WHERE lower(parse_url(license.type, "HOST")) = "creativecommons.org") tmp
                         on r.id= tmp.id;

drop table if exists ${stats_db_name}.indi_pub_has_abstract purge;

create table if not exists ${stats_db_name}.indi_pub_has_abstract stored as parquet as
select distinct publication.id, cast(coalesce(abstract, true) as int) has_abstract
from ${stats_db_name}.publication;

drop table if exists ${stats_db_name}.indi_result_with_orcid purge;

create table if not exists ${stats_db_name}.indi_result_with_orcid stored as parquet as
select distinct r.id, coalesce(has_orcid, 0) as has_orcid
from ${stats_db_name}.result r
         left outer join (select id, 1 as has_orcid from ${stats_db_name}.result_orcid) tmp
                         on r.id= tmp.id;

---- Sprint 3 ----

drop table if exists ${stats_db_name}.indi_funded_result_with_fundref purge;

create table if not exists ${stats_db_name}.indi_funded_result_with_fundref stored as parquet as
select distinct r.result as id, coalesce(fundref, 0) as fundref
from ${stats_db_name}.project_results r
         left outer join (select distinct result, 1 as fundref from ${stats_db_name}.project_results
                          where provenance='Harvested') tmp
                         on r.result= tmp.result;

-- create table indi_result_org_collab stored as parquet as
-- select o1.organization org1, o2.organization org2, count(distinct o1.id) as collaborations
-- from result_organization as o1
--          join result_organization as o2 on o1.id=o2.id and o1.organization!=o2.organization
-- group by o1.organization, o2.organization;
--
-- compute stats indi_result_org_collab;
--
create TEMPORARY TABLE ${stats_db_name}.tmp AS SELECT ro.organization organization, ro.id, o.name from ${stats_db_name}.result_organization ro
join ${stats_db_name}.organization o on o.id=ro.organization where o.name is not null;

drop table if exists ${stats_db_name}.indi_result_org_collab purge;

create table if not exists ${stats_db_name}.indi_result_org_collab stored as parquet as
select o1.organization org1, o1.name org1name1, o2.organization org2, o2.name org2name2, count(o1.id) as collaborations
from ${stats_db_name}.tmp as o1
join ${stats_db_name}.tmp as o2 where o1.id=o2.id and o1.organization!=o2.organization and o1.name!=o2.name
group by o1.organization, o2.organization, o1.name, o2.name;

drop table if exists ${stats_db_name}.tmp purge;

create TEMPORARY TABLE ${stats_db_name}.tmp AS
select distinct ro.organization organization, ro.id, o.name, o.country from ${stats_db_name}.result_organization ro
join ${stats_db_name}.organization o on o.id=ro.organization where country <> 'UNKNOWN'  and o.name is not null;

drop table if exists ${stats_db_name}.indi_result_org_country_collab purge;

create table if not exists ${stats_db_name}.indi_result_org_country_collab stored as parquet as
select o1.organization org1,o1.name org1name1, o2.country country2, count(o1.id) as collaborations
from ${stats_db_name}.tmp as o1 join ${stats_db_name}.tmp as o2 on o1.id=o2.id
where o1.id=o2.id and o1.country!=o2.country
group by o1.organization, o1.id, o1.name, o2.country;

drop table if exists  ${stats_db_name}.tmp purge;

create TEMPORARY TABLE ${stats_db_name}.tmp AS
select o.id organization, o.name, ro.project as project  from ${stats_db_name}.organization o
        join ${stats_db_name}.organization_projects ro on o.id=ro.id  where o.name is not null;

drop table if exists ${stats_db_name}.indi_project_collab_org purge;

create table if not exists ${stats_db_name}.indi_project_collab_org stored as parquet as
select o1.organization org1,o1.name orgname1, o2.organization org2, o2.name orgname2, count(distinct o1.project) as collaborations
from ${stats_db_name}.tmp as o1
         join ${stats_db_name}.tmp as o2 on o1.project=o2.project
where o1.organization<>o2.organization and o1.name<>o2.name
group by o1.name,o2.name, o1.organization, o2.organization;

drop table if exists ${stats_db_name}.tmp purge;

create TEMPORARY TABLE ${stats_db_name}.tmp AS
select o.id organization, o.name, o.country , ro.project as project  from ${stats_db_name}.organization o
        join ${stats_db_name}.organization_projects ro on o.id=ro.id
        and o.country <> 'UNKNOWN' and o.name is not null;

drop table if exists ${stats_db_name}.indi_project_collab_org_country purge;

create table if not exists ${stats_db_name}.indi_project_collab_org_country stored as parquet as
select o1.organization org1,o1.name org1name, o2.country country2, count(distinct o1.project) as collaborations
from ${stats_db_name}.tmp as o1
         join ${stats_db_name}.tmp as o2 on o1.project=o2.project
where o1.organization<>o2.organization and o1.country<>o2.country
group by o1.organization, o2.country, o1.name;

drop table if exists ${stats_db_name}.tmp purge;

drop table if exists ${stats_db_name}.indi_funder_country_collab purge;

create table if not exists ${stats_db_name}.indi_funder_country_collab stored as parquet as
    with tmp as (select funder, project, country from ${stats_db_name}.organization_projects op
        join ${stats_db_name}.organization o on o.id=op.id
        join ${stats_db_name}.project p on p.id=op.project
        where country <> 'UNKNOWN')
select f1.funder, f1.country as country1, f2.country as country2, count(distinct f1.project) as collaborations
from tmp as f1
         join tmp as f2 on f1.project=f2.project
where f1.country<>f2.country
group by f1.funder, f2.country, f1.country;

create TEMPORARY TABLE ${stats_db_name}.tmp AS
select distinct country, ro.id as result  from ${stats_db_name}.organization o
        join ${stats_db_name}.result_organization ro on o.id=ro.organization
        where country <> 'UNKNOWN' and o.name is not null;

drop table if exists ${stats_db_name}.indi_result_country_collab purge;

create table if not exists ${stats_db_name}.indi_result_country_collab stored as parquet as
select o1.country country1, o2.country country2, count(o1.result) as collaborations
from ${stats_db_name}.tmp as o1
         join ${stats_db_name}.tmp as o2 on o1.result=o2.result
where o1.country<>o2.country
group by o1.country, o2.country;

drop table if exists ${stats_db_name}.tmp purge;

---- Sprint 4 ----
drop table if exists ${stats_db_name}.indi_pub_diamond purge;

create table if not exists ${stats_db_name}.indi_pub_diamond stored as parquet as
select distinct pd.id, coalesce(in_diamond_journal, 0) as in_diamond_journal
from ${stats_db_name}.publication_datasources pd
         left outer join (
    select pd.id, 1 as in_diamond_journal from ${stats_db_name}.publication_datasources pd
                                                   join ${stats_db_name}.datasource d on d.id=pd.datasource
                                                   join STATS_EXT.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
                                                                                 and (ps.journal_is_in_doaj=true or ps.journal_is_oa=true) and ps.has_apc=false) tmp
                         on pd.id=tmp.id;

drop table if exists ${stats_db_name}.indi_pub_in_transformative purge;

create table if not exists ${stats_db_name}.indi_pub_in_transformative stored as parquet as
select distinct pd.id, coalesce(is_transformative, 0) as is_transformative
from ${stats_db_name}.publication pd
         left outer join (
    select  pd.id, 1 as is_transformative from ${stats_db_name}.publication_datasources pd
                                                   join ${stats_db_name}.datasource d on d.id=pd.datasource
                                                   join STATS_EXT.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
                                                                                 and ps.is_transformative_journal=true) tmp
                         on pd.id=tmp.id;

drop table if exists ${stats_db_name}.indi_pub_closed_other_open purge;

create table if not exists ${stats_db_name}.indi_pub_closed_other_open stored as parquet as
select distinct ri.id, coalesce(pub_closed_other_open, 0) as pub_closed_other_open from ${stats_db_name}.result_instance ri
                                                                                            left outer join
                                                                                        (select ri.id, 1 as pub_closed_other_open from ${stats_db_name}.result_instance ri
                                                                                                                                           join ${stats_db_name}.publication p on p.id=ri.id
                                                                                                                                           join ${stats_db_name}.datasource d on ri.hostedby=d.id
                                                                                         where d.type like '%Journal%' and ri.accessright='Closed Access' and
                                                                                             (p.bestlicence='Open Access' or p.bestlicence='Open Source')) tmp
                                                                                        on tmp.id=ri.id;

---- Sprint 5 ----
drop table if exists ${stats_db_name}.indi_result_no_of_copies purge;

create table if not exists ${stats_db_name}.indi_result_no_of_copies stored as parquet as
select id, count(id) as number_of_copies from ${stats_db_name}.result_instance group by id;

---- Sprint 6 ----
drop table if exists ${stats_db_name}.indi_pub_downloads purge;

create table if not exists ${stats_db_name}.indi_pub_downloads stored as parquet as
SELECT result_id, sum(downloads) no_downloads from openaire_prod_usage_stats.usage_stats
                                                      join ${stats_db_name}.publication on result_id=id
where downloads>0
GROUP BY result_id
order by no_downloads desc;

--ANALYZE TABLE ${stats_db_name}.indi_pub_downloads COMPUTE STATISTICS;

drop table if exists ${stats_db_name}.indi_pub_downloads_datasource purge;

create table if not exists ${stats_db_name}.indi_pub_downloads_datasource stored as parquet as
SELECT result_id, repository_id, sum(downloads) no_downloads from openaire_prod_usage_stats.usage_stats
                                                                     join ${stats_db_name}.publication on result_id=id
where downloads>0
GROUP BY result_id, repository_id
order by result_id;

drop table if exists ${stats_db_name}.indi_pub_downloads_year purge;

create table if not exists ${stats_db_name}.indi_pub_downloads_year stored as parquet as
SELECT result_id, cast(substring(us.`date`, 1,4) as int) as `year`, sum(downloads) no_downloads
from openaire_prod_usage_stats.usage_stats us
join ${stats_db_name}.publication on result_id=id where downloads>0
GROUP BY result_id, substring(us.`date`, 1,4);

drop table if exists ${stats_db_name}.indi_pub_downloads_datasource_year purge;

create table if not exists ${stats_db_name}.indi_pub_downloads_datasource_year stored as parquet as
SELECT result_id, cast(substring(us.`date`, 1,4) as int) as `year`, repository_id, sum(downloads) no_downloads from openaire_prod_usage_stats.usage_stats us
join ${stats_db_name}.publication on result_id=id
where downloads>0
GROUP BY result_id, repository_id, substring(us.`date`, 1,4);

---- Sprint 7 ----
drop table if exists ${stats_db_name}.indi_pub_gold_oa purge;

--create table if not exists ${stats_db_name}.indi_pub_gold_oa stored as parquet as
--    WITH gold_oa AS (    SELECT
--        issn_l,
--        journal_is_in_doaj,
--        journal_is_oa,
--        issn_1 as issn
--        FROM
--        STATS_EXT.oa_journals
--        WHERE
--        issn_1 != ""
--        UNION
--        ALL SELECT
--        issn_l,
--        journal_is_in_doaj,
--        journal_is_oa,
--        issn_2 as issn
--        FROM
--        STATS_EXT.oa_journals
--        WHERE
--        issn_2 != "" ),  issn AS ( SELECT
--                                   *
--                                   FROM
--( SELECT
--                                   id,
--                                   issn_printed as issn
--                                   FROM
--                                   ${stats_db_name}.datasource
--                                   WHERE
--                                   issn_printed IS NOT NULL
--                                   UNION ALL
--                                   SELECT
--                                   id,
--                                   issn_online as issn
--                                   FROM
--                                   ${stats_db_name}.datasource
--                                   WHERE
--                                   issn_online IS NOT NULL or id like '%doajarticles%') as issn
--    WHERE
--    LENGTH(issn) > 7)
--SELECT
--    DISTINCT pd.id, coalesce(is_gold, 0) as is_gold
--FROM
--    ${stats_db_name}.publication_datasources pd
--        left outer join(
--        select pd.id, 1 as is_gold FROM ${stats_db_name}.publication_datasources pd
--                                            JOIN issn on issn.id=pd.datasource
--                                            JOIN gold_oa  on issn.issn = gold_oa.issn) tmp
--                       on pd.id=tmp.id;

create table if not exists ${stats_db_name}.indi_pub_gold_oa stored as parquet as
with gold_oa as (
SELECT issn,issn_l from stats_ext.issn_gold_oa_dataset_v5),
issn AS (SELECT * FROM
(SELECT id,issn_printed as issn FROM ${stats_db_name}.datasource
WHERE issn_printed IS NOT NULL
UNION ALL
SELECT id, issn_online as issn FROM ${stats_db_name}.datasource
WHERE issn_online IS NOT NULL or id like '%doajarticles%') as issn
WHERE LENGTH(issn) > 7),
alljournals AS(select issn, issn_l from stats_ext.alljournals
where journal_is_in_doaj=true or journal_is_oa=true)
SELECT DISTINCT pd.id, coalesce(is_gold, 0) as is_gold
FROM ${stats_db_name}.publication_datasources pd
left outer join (
select pd.id, 1 as is_gold FROM ${stats_db_name}.publication_datasources pd
JOIN issn on issn.id=pd.datasource
JOIN gold_oa  on issn.issn = gold_oa.issn
join alljournals on issn.issn=alljournals.issn
left outer join ${stats_db_name}.result_instance ri on ri.id=pd.id
and ri.accessright!='Closed Access' and ri.accessright_uw='gold') tmp
on pd.id=tmp.id;

drop table if exists ${stats_db_name}.indi_pub_hybrid_oa_with_cc purge;

create table if not exists ${stats_db_name}.indi_pub_hybrid_oa_with_cc stored as parquet as
    WITH hybrid_oa AS (
        SELECT issn_l, journal_is_in_doaj, journal_is_oa, issn_print as issn
        FROM STATS_EXT.plan_s_jn
        WHERE issn_print != ""
        UNION ALL
        SELECT issn_l, journal_is_in_doaj, journal_is_oa, issn_online as issn
        FROM STATS_EXT.plan_s_jn
        WHERE issn_online != "" and (journal_is_in_doaj = FALSE OR journal_is_oa = FALSE)),
    issn AS (
                SELECT *
                FROM (
                SELECT id, issn_printed as issn
                FROM ${stats_db_name}.datasource
                WHERE issn_printed IS NOT NULL
                UNION ALL
                SELECT id,issn_online as issn
                FROM ${stats_db_name}.datasource
                WHERE issn_online IS NOT NULL ) as issn
    WHERE LENGTH(issn) > 7)
SELECT DISTINCT pd.id, coalesce(is_hybrid_oa, 0) as is_hybrid_oa
FROM ${stats_db_name}.publication_datasources pd
         LEFT OUTER JOIN (
    SELECT pd.id, 1 as is_hybrid_oa from ${stats_db_name}.publication_datasources pd
                                             JOIN ${stats_db_name}.datasource d on d.id=pd.datasource
                                             JOIN issn on issn.id=pd.datasource
                                             JOIN hybrid_oa ON issn.issn = hybrid_oa.issn
                                             JOIN ${stats_db_name}.indi_result_has_cc_licence cc on pd.id=cc.id
                                             JOIN ${stats_db_name}.indi_pub_gold_oa ga on pd.id=ga.id
    where cc.has_cc_license=1 and ga.is_gold=0) tmp on pd.id=tmp.id;

drop table if exists ${stats_db_name}.indi_pub_hybrid purge;

--create table if not exists ${stats_db_name}.indi_pub_hybrid stored as parquet as
--    WITH gold_oa AS ( SELECT
--        issn_l,
--        journal_is_in_doaj,
--        journal_is_oa,
--        issn_1 as issn,
--        has_apc
--        FROM
--        STATS_EXT.oa_journals
--        WHERE
--        issn_1 != ""
--        UNION
--        ALL SELECT
--        issn_l,
--        journal_is_in_doaj,
--        journal_is_oa,
--        issn_2 as issn,
--        has_apc
--        FROM
--        STATS_EXT.oa_journals
--        WHERE
--        issn_2 != "" ),  issn AS ( SELECT
--                                   *
--                                   FROM
--( SELECT
--                                   id,
--                                   issn_printed as issn
--                                   FROM
--                                   ${stats_db_name}.datasource
--                                   WHERE
--                                   issn_printed IS NOT NULL
--                                   UNION ALL
--                                   SELECT
--                                   id,
--                                   issn_online as issn
--                                   FROM
--                                   ${stats_db_name}.datasource
--                                   WHERE
--                                   issn_online IS NOT NULL or id like '%doajarticles%') as issn
--    WHERE
--    LENGTH(issn) > 7)
--select distinct pd.id, coalesce(is_hybrid, 0) as is_hybrid
--from ${stats_db_name}.publication_datasources pd
--         left outer join (
--    select pd.id, 1 as is_hybrid from ${stats_db_name}.publication_datasources pd
--                                          join ${stats_db_name}.datasource d on d.id=pd.datasource
--                                          join issn on issn.id=pd.datasource
--                                          join gold_oa on issn.issn=gold_oa.issn
--    where (gold_oa.journal_is_in_doaj=false or gold_oa.journal_is_oa=false))tmp
--                         on pd.id=tmp.id;

create table if not exists ${stats_db_name}.indi_pub_hybrid stored as parquet as
select distinct pd.id,coalesce(is_hybrid,0) is_hybrid from ${stats_db_name}.publication_datasources pd
left outer join (select pd.id, 1 as is_hybrid from ${stats_db_name}.publication_datasources pd
join ${stats_db_name}.datasource d on pd.datasource=d.id
join ${stats_db_name}.result_instance ri on ri.id=pd.id
join ${stats_db_name}.indi_pub_gold_oa indi_gold on indi_gold.id=pd.id
join ${stats_db_name}.result_accessroute ra on ra.id=pd.id
where d.type like '%Journal%' and ri.accessright!='Closed Access' and (ri.accessright_uw!='gold'
or indi_gold.is_gold=0) and (ra.accessroute='hybrid' or ri.license is not null)) tmp
on pd.id=tmp.id;

drop table if exists ${stats_db_name}.indi_org_fairness purge;

create table if not exists ${stats_db_name}.indi_org_fairness stored as parquet as
--return results with PIDs, and rich metadata group by organization
    with result_fair as
        (select ro.organization organization, count(distinct ro.id) no_result_fair from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
--join result_pids rp on r.id=rp.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
    group by ro.organization),
--return all results group by organization
    allresults as (select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    where  cast(year as int)>2003
    group by ro.organization)
--return results_fair/all_results
select allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
from allresults
         join result_fair on result_fair.organization=allresults.organization;

CREATE TEMPORARY table ${stats_db_name}.result_fair as
select ro.organization organization, count(distinct ro.id) no_result_fair
    from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication p on p.id=ro.id
    join ${stats_db_name}.indi_pub_doi_from_crossref dc on dc.id=p.id
    join ${stats_db_name}.indi_pub_grey_lit gl on gl.id=p.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null)
    and (authors>0) and cast(year as int)>2003 and dc.doi_from_crossref=1 and gl.grey_lit=0
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.allresults as
select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication p on p.id=ro.id
    where cast(year as int)>2003
    group by ro.organization;

drop table if exists ${stats_db_name}.indi_org_fairness_pub_pr purge;

create table if not exists ${stats_db_name}.indi_org_fairness_pub_pr stored as parquet as
select ar.organization, rf.no_result_fair/ar.no_allresults org_fairness
from ${stats_db_name}.allresults ar
         join ${stats_db_name}.result_fair rf on rf.organization=ar.organization;

DROP table ${stats_db_name}.result_fair purge;
DROP table ${stats_db_name}.allresults purge;

CREATE TEMPORARY table ${stats_db_name}.result_fair as
    select year, ro.organization organization, count(distinct ro.id) no_result_fair from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result p on p.id=ro.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and cast(year as int)>2003
    group by ro.organization, year;

CREATE TEMPORARY TABLE ${stats_db_name}.allresults as select year, ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result p on p.id=ro.id
    where cast(year as int)>2003
    group by ro.organization, year;

drop table if exists ${stats_db_name}.indi_org_fairness_pub_year purge;

create table if not exists ${stats_db_name}.indi_org_fairness_pub_year stored as parquet as
select cast(allresults.year as int) year, allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
from ${stats_db_name}.allresults
         join ${stats_db_name}.result_fair on result_fair.organization=allresults.organization and result_fair.year=allresults.year;

DROP table ${stats_db_name}.result_fair purge;
DROP table ${stats_db_name}.allresults purge;

CREATE TEMPORARY TABLE ${stats_db_name}.result_fair as
    select ro.organization organization, count(distinct ro.id) no_result_fair
     from ${stats_db_name}.result_organization ro
              join ${stats_db_name}.result p on p.id=ro.id
     where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null)
       and (authors>0) and cast(year as int)>2003
     group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.allresults as
    select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result p on p.id=ro.id
    where cast(year as int)>2003
    group by ro.organization;

drop table if exists ${stats_db_name}.indi_org_fairness_pub purge;

create table if not exists ${stats_db_name}.indi_org_fairness_pub as
select ar.organization, rf.no_result_fair/ar.no_allresults org_fairness
from ${stats_db_name}.allresults ar join ${stats_db_name}.result_fair rf
on rf.organization=ar.organization;

DROP table ${stats_db_name}.result_fair purge;
DROP table ${stats_db_name}.allresults purge;

CREATE TEMPORARY TABLE ${stats_db_name}.result_fair as
    select year, ro.organization organization, count(distinct ro.id) no_result_fair from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    join ${stats_db_name}.result_pids rp on r.id=rp.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
    group by ro.organization, year;

CREATE TEMPORARY TABLE ${stats_db_name}.allresults as
    select year, ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    where  cast(year as int)>2003
    group by ro.organization, year;

drop table if exists ${stats_db_name}.indi_org_fairness_year purge;

create table if not exists ${stats_db_name}.indi_org_fairness_year stored as parquet as
    select cast(allresults.year as int) year, allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
    from ${stats_db_name}.allresults
    join ${stats_db_name}.result_fair on result_fair.organization=allresults.organization and cast(result_fair.year as int)=cast(allresults.year as int);

DROP table ${stats_db_name}.result_fair purge;
DROP table ${stats_db_name}.allresults purge;

CREATE TEMPORARY TABLE ${stats_db_name}.result_with_pid as
    select year, ro.organization, count(distinct rp.id) no_result_with_pid from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result_pids rp on rp.id=ro.id
    join ${stats_db_name}.result r on r.id=rp.id
    where cast(year as int) >2003
    group by ro.organization, year;

CREATE TEMPORARY TABLE ${stats_db_name}.allresults as
    select year, ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    where cast(year as int) >2003
    group by ro.organization, year;

drop table if exists ${stats_db_name}.indi_org_findable_year purge;

create table if not exists ${stats_db_name}.indi_org_findable_year stored as parquet as
select cast(allresults.year as int) year, allresults.organization, result_with_pid.no_result_with_pid/allresults.no_allresults org_findable
from ${stats_db_name}.allresults
         join ${stats_db_name}.result_with_pid on result_with_pid.organization=allresults.organization and cast(result_with_pid.year as int)=cast(allresults.year as int);

DROP table ${stats_db_name}.result_with_pid purge;
DROP table ${stats_db_name}.allresults purge;

CREATE TEMPORARY TABLE ${stats_db_name}.result_with_pid as
select ro.organization, count(distinct rp.id) no_result_with_pid from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result_pids rp on rp.id=ro.id
    join ${stats_db_name}.result r on r.id=rp.id
    where cast(year as int) >2003
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.allresults as
select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    where cast(year as int) >2003
    group by ro.organization;

drop table if exists ${stats_db_name}.indi_org_findable purge;

create table if not exists ${stats_db_name}.indi_org_findable stored as parquet as
select allresults.organization, result_with_pid.no_result_with_pid/allresults.no_allresults org_findable
from ${stats_db_name}.allresults
         join ${stats_db_name}.result_with_pid on result_with_pid.organization=allresults.organization;

DROP table ${stats_db_name}.result_with_pid purge;
DROP table ${stats_db_name}.allresults purge;

CREATE TEMPORARY TABLE ${stats_db_name}.pubs_oa as
SELECT ro.organization, count(distinct r.id) no_oapubs FROM ${stats_db_name}.publication r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.datasets_oa as
SELECT ro.organization, count(distinct r.id) no_oadatasets FROM ${stats_db_name}.dataset r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.software_oa as
SELECT ro.organization, count(distinct r.id) no_oasoftware FROM ${stats_db_name}.software r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubs as
SELECT ro.organization, count(ro.id) no_allpubs FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasets as
SELECT ro.organization, count(ro.id) no_alldatasets FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.dataset ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftware as
SELECT ro.organization, count(ro.id) no_allsoftware FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.software ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubsshare as
select pubs_oa.organization, pubs_oa.no_oapubs/allpubs.no_allpubs p from ${stats_db_name}.allpubs
                        join ${stats_db_name}.pubs_oa on allpubs.organization=pubs_oa.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasetssshare as
select datasets_oa.organization, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from ${stats_db_name}.alldatasets
                             join ${stats_db_name}.datasets_oa on alldatasets.organization=datasets_oa.organization;

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftwaresshare as
select software_oa.organization, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from ${stats_db_name}.allsoftware
                             join ${stats_db_name}.software_oa on allsoftware.organization=software_oa.organization;

drop table if exists ${stats_db_name}.indi_org_openess purge;

create table if not exists ${stats_db_name}.indi_org_openess stored as parquet as
select allpubsshare.organization,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           org_openess FROM ${stats_db_name}.allpubsshare
                                left outer join (select organization,d from
    ${stats_db_name}.alldatasetssshare) tmp1
                                                on tmp1.organization=allpubsshare.organization
                                left outer join (select organization,s from
    ${stats_db_name}.allsoftwaresshare) tmp2
                                                on tmp2.organization=allpubsshare.organization;

DROP TABLE ${stats_db_name}.pubs_oa purge;
DROP TABLE ${stats_db_name}.datasets_oa purge;
DROP TABLE ${stats_db_name}.software_oa purge;
DROP TABLE ${stats_db_name}.allpubs purge;
DROP TABLE ${stats_db_name}.alldatasets purge;
DROP TABLE ${stats_db_name}.allsoftware purge;
DROP TABLE ${stats_db_name}.allpubsshare purge;
DROP TABLE ${stats_db_name}.alldatasetssshare purge;
DROP TABLE ${stats_db_name}.allsoftwaresshare purge;

CREATE TEMPORARY TABLE ${stats_db_name}.pubs_oa AS
SELECT r.year, ro.organization, count(distinct r.id) no_oapubs FROM ${stats_db_name}.publication r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization,r.year;

CREATE TEMPORARY TABLE ${stats_db_name}.datasets_oa AS
SELECT r.year,ro.organization, count(distinct r.id) no_oadatasets FROM ${stats_db_name}.dataset r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization, r.year;

CREATE TEMPORARY TABLE ${stats_db_name}.software_oa AS
SELECT r.year,ro.organization, count(distinct r.id) no_oasoftware FROM ${stats_db_name}.software r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization, r.year;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubs as
SELECT p.year,ro.organization organization, count(ro.id) no_allpubs FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication p on p.id=ro.id where cast(p.year as int)>2003
    group by ro.organization, p.year;

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasets as
SELECT d.year, ro.organization organization, count(ro.id) no_alldatasets FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.dataset d on d.id=ro.id where cast(d.year as int)>2003
    group by ro.organization, d.year;

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftware as
SELECT s.year,ro.organization organization, count(ro.id) no_allsoftware FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.software s on s.id=ro.id where cast(s.year as int)>2003
    group by ro.organization, s.year;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubsshare as
select allpubs.year, pubs_oa.organization, pubs_oa.no_oapubs/allpubs.no_allpubs p from ${stats_db_name}.allpubs
                        join ${stats_db_name}.pubs_oa on allpubs.organization=pubs_oa.organization where cast(allpubs.year as INT)=cast(pubs_oa.year as int);

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasetssshare as
select alldatasets.year, datasets_oa.organization, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from ${stats_db_name}.alldatasets
                             join ${stats_db_name}.datasets_oa on alldatasets.organization=datasets_oa.organization where cast(alldatasets.year as INT)=cast(datasets_oa.year as int);

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftwaresshare as
select allsoftware.year, software_oa.organization, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from ${stats_db_name}.allsoftware
                             join ${stats_db_name}.software_oa on allsoftware.organization=software_oa.organization where cast(allsoftware.year as INT)=cast(software_oa.year as int);

drop table if exists ${stats_db_name}.indi_org_openess_year purge;

create table if not exists ${stats_db_name}.indi_org_openess_year stored as parquet as
select cast(allpubsshare.year as int) year, allpubsshare.organization,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           org_openess FROM ${stats_db_name}.allpubsshare
                                left outer join (select cast(year as int), organization,d from
    ${stats_db_name}.alldatasetssshare) tmp1
                                                on tmp1.organization=allpubsshare.organization and tmp1.year=allpubsshare.year
                                left outer join (select cast(year as int), organization,s from
    ${stats_db_name}.allsoftwaresshare) tmp2
                                                on tmp2.organization=allpubsshare.organization and cast(tmp2.year as int)=cast(allpubsshare.year as int);

DROP TABLE ${stats_db_name}.pubs_oa purge;
DROP TABLE ${stats_db_name}.datasets_oa purge;
DROP TABLE ${stats_db_name}.software_oa purge;
DROP TABLE ${stats_db_name}.allpubs purge;
DROP TABLE ${stats_db_name}.alldatasets purge;
DROP TABLE ${stats_db_name}.allsoftware purge;
DROP TABLE ${stats_db_name}.allpubsshare purge;
DROP TABLE ${stats_db_name}.alldatasetssshare purge;
DROP TABLE ${stats_db_name}.allsoftwaresshare purge;

drop table if exists ${stats_db_name}.indi_pub_has_preprint purge;

create table if not exists ${stats_db_name}.indi_pub_has_preprint stored as parquet as
select distinct p.id, coalesce(has_preprint, 0) as has_preprint
from ${stats_db_name}.publication_classifications p
         left outer join (
    select p.id, 1 as has_preprint
    from ${stats_db_name}.publication_classifications p
    where p.type='Preprint') tmp
                         on p.id= tmp.id;
drop table if exists ${stats_db_name}.indi_pub_in_subscribed purge;

create table if not exists ${stats_db_name}.indi_pub_in_subscribed stored as parquet as
select distinct p.id, coalesce(is_subscription, 0) as is_subscription
from ${stats_db_name}.publication p
         left outer join(
    select  p.id, 1 as is_subscription from ${stats_db_name}.publication p
                                                join ${stats_db_name}.indi_pub_gold_oa g on p.id=g.id
                                                join ${stats_db_name}.indi_pub_hybrid h on p.id=h.id
                                                join ${stats_db_name}.indi_pub_in_transformative t on p.id=t.id
    where g.is_gold=0 and h.is_hybrid=0 and t.is_transformative=0) tmp
                        on p.id=tmp.id;

drop table if exists ${stats_db_name}.indi_result_with_pid purge;

create table if not exists ${stats_db_name}.indi_result_with_pid as
select distinct p.id, coalesce(result_with_pid, 0) as result_with_pid
from ${stats_db_name}.result p
         left outer join (
    select p.id, 1 as result_with_pid
    from ${stats_db_name}.result_pids p) tmp
                         on p.id= tmp.id;

CREATE TEMPORARY TABLE ${stats_db_name}.pub_fos_totals as
select rf.id, count(distinct lvl3) totals from ${stats_db_name}.result_fos rf
group by rf.id;

drop table if exists ${stats_db_name}.indi_pub_interdisciplinarity purge;

create table if not exists ${stats_db_name}.indi_pub_interdisciplinarity as
select distinct p.id as id, coalesce(is_interdisciplinary, 0)
as is_interdisciplinary
from ${stats_db_name}.pub_fos_totals p
left outer join (
select pub_fos_totals.id, 1 as is_interdisciplinary from ${stats_db_name}.pub_fos_totals
where totals>1) tmp on p.id=tmp.id;

drop table ${stats_db_name}.pub_fos_totals purge;

drop table if exists ${stats_db_name}.indi_pub_bronze_oa purge;

--create table if not exists ${stats_db_name}.indi_pub_bronze_oa stored as parquet as
--select distinct p.id, coalesce(is_bronze_oa,0) as is_bronze_oa
--from ${stats_db_name}.publication p
--left outer join
--(select p.id, 1 as is_bronze_oa from ${stats_db_name}.publication p
--join ${stats_db_name}.indi_result_has_cc_licence cc on cc.id=p.id
--join ${stats_db_name}.indi_pub_gold_oa ga on ga.id=p.id
--join ${stats_db_name}.result_instance ri on ri.id=p.id
--join ${stats_db_name}.datasource d on d.id=ri.hostedby
--where cc.has_cc_license=0 and ga.is_gold=0
--and (d.type='Journal' or d.type='Journal Aggregator/Publisher')
--and ri.accessright='Open Access') tmp on tmp.id=p.id;

create table ${stats_db_name}.indi_pub_bronze_oa stored as parquet as
select distinct pd.id,coalesce(is_bronze_oa,0) is_bronze_oa from ${stats_db_name}.publication_datasources pd
left outer join (select pd.id, 1 as is_bronze_oa from ${stats_db_name}.publication_datasources pd
join ${stats_db_name}.datasource d on pd.datasource=d.id
join ${stats_db_name}.result_instance ri on ri.id=pd.id
join ${stats_db_name}.indi_pub_gold_oa indi_gold on indi_gold.id=pd.id
join ${stats_db_name}.result_accessroute ra on ra.id=pd.id
where d.type like '%Journal%' and ri.accessright!='Closed Access' and (ri.accessright_uw!='gold'
or indi_gold.is_gold=0) and (ra.accessroute='bronze' or ri.license is null)) tmp
on pd.id=tmp.id;

CREATE TEMPORARY TABLE ${stats_db_name}.project_year_result_year as
select p.id project_id, acronym, r.id result_id, r.year, p.end_year
from ${stats_db_name}.project p
join ${stats_db_name}.result_projects rp on p.id=rp.project
join ${stats_db_name}.result r on r.id=rp.id
where p.end_year is NOT NULL and r.year is not null;

drop table if exists ${stats_db_name}.indi_is_project_result_after purge;

create table if not exists ${stats_db_name}.indi_is_project_result_after stored as parquet as
select pry.project_id, pry.acronym, pry.result_id,
coalesce(is_project_result_after, 0) as is_project_result_after
from ${stats_db_name}.project_year_result_year pry
left outer join (select pry.project_id, pry.acronym, pry.result_id, 1 as is_project_result_after
from ${stats_db_name}.project_year_result_year pry
where pry.year>pry.end_year) tmp on pry.result_id=tmp.result_id;

drop table ${stats_db_name}.project_year_result_year purge;

drop table ${stats_db_name}.indi_is_funder_plan_s purge;

create table if not exists ${stats_db_name}.indi_is_funder_plan_s stored as parquet as
select distinct f.id, f.name, coalesce(is_funder_plan_s, 0) as is_funder_plan_s
from ${stats_db_name}.funder f
         left outer join (select id, name, 1 as is_funder_plan_s from ${stats_db_name}.funder
         join stats_ext.plan_s_short on c_o_alition_s_organisation_funder=name) tmp
                         on f.name= tmp.name;

--Funder Fairness
drop table ${stats_db_name}.indi_funder_fairness purge;

create table if not exists ${stats_db_name}.indi_funder_fairness stored as parquet as
    with result_fair as
        (select p.funder funder, count(distinct rp.id) no_result_fair from ${stats_db_name}.result_projects rp
    join ${stats_db_name}.result r on r.id=rp.id
    join ${stats_db_name}.project p on p.id=rp.project
    where (r.title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
    group by p.funder),
    allresults as (select p.funder funder, count(distinct rp.id) no_allresults from ${stats_db_name}.result_projects rp
    join ${stats_db_name}.result r on r.id=rp.id
    join ${stats_db_name}.project p on p.id=rp.project
    where  cast(year as int)>2003
    group by p.funder)
select allresults.funder, result_fair.no_result_fair/allresults.no_allresults funder_fairness
from allresults
         join result_fair on result_fair.funder=allresults.funder;

--RIs Fairness
drop table ${stats_db_name}.indi_ris_fairness purge;

create table if not exists ${stats_db_name}.indi_ris_fairness stored as parquet as
with result_contexts as
(select distinct rc.id, context.name ri_initiative from ${stats_db_name}.result_concepts rc
join ${stats_db_name}.concept on concept.id=rc.concept
join ${stats_db_name}.category on category.id=concept.category
join ${stats_db_name}.context on context.id=category.context),
result_fair as
        (select rc.ri_initiative ri_initiative, count(distinct rc.id) no_result_fair from result_contexts rc
    join ${stats_db_name}.result r on r.id=rc.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
    group by rc.ri_initiative),
allresults as
(select rc.ri_initiative ri_initiative, count(distinct rc.id) no_allresults from result_contexts rc
    join ${stats_db_name}.result r on r.id=rc.id
    where  cast(year as int)>2003
    group by rc.ri_initiative)
select allresults.ri_initiative, result_fair.no_result_fair/allresults.no_allresults ris_fairness
from allresults
         join result_fair on result_fair.ri_initiative=allresults.ri_initiative;

--Funder Openess

CREATE TEMPORARY TABLE ${stats_db_name}.pubs_oa as
select p.funder funder, count(distinct rp.id) no_oapubs from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.publication r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by p.funder;


CREATE TEMPORARY TABLE ${stats_db_name}.datasets_oa as
select p.funder funder, count(distinct rp.id) no_oadatasets from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.dataset r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by p.funder;

CREATE TEMPORARY TABLE ${stats_db_name}.software_oa as
select p.funder funder, count(distinct rp.id) no_oasoftware from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.software r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by p.funder;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubs as
select p.funder funder, count(distinct rp.id) no_allpubs from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.publication r on r.id=rp.id
where cast(r.year as int)>2003
group by p.funder;

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasets as
select p.funder funder, count(distinct rp.id) no_alldatasets from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.dataset r on r.id=rp.id
where cast(r.year as int)>2003
group by p.funder;

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftware as
select p.funder funder, count(distinct rp.id) no_allsoftware from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.software r on r.id=rp.id
where cast(r.year as int)>2003
group by p.funder;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubsshare as
select pubs_oa.funder, pubs_oa.no_oapubs/allpubs.no_allpubs p from ${stats_db_name}.allpubs
                        join ${stats_db_name}.pubs_oa on allpubs.funder=pubs_oa.funder;

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasetssshare as
select datasets_oa.funder, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from ${stats_db_name}.alldatasets
                             join ${stats_db_name}.datasets_oa on alldatasets.funder=datasets_oa.funder;

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftwaresshare as
select software_oa.funder, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from ${stats_db_name}.allsoftware
                             join ${stats_db_name}.software_oa on allsoftware.funder=software_oa.funder;

drop table ${stats_db_name}.indi_funder_openess purge;

create table if not exists ${stats_db_name}.indi_funder_openess stored as parquet as
select allpubsshare.funder,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           funder_openess FROM ${stats_db_name}.allpubsshare
                                left outer join (select funder,d from
    ${stats_db_name}.alldatasetssshare) tmp1
                                                on tmp1.funder=allpubsshare.funder
                                left outer join (select funder,s from
    ${stats_db_name}.allsoftwaresshare) tmp2
                                                on tmp2.funder=allpubsshare.funder;

DROP TABLE ${stats_db_name}.pubs_oa purge;
DROP TABLE ${stats_db_name}.datasets_oa purge;
DROP TABLE ${stats_db_name}.software_oa purge;
DROP TABLE ${stats_db_name}.allpubs purge;
DROP TABLE ${stats_db_name}.alldatasets purge;
DROP TABLE ${stats_db_name}.allsoftware purge;
DROP TABLE ${stats_db_name}.allpubsshare purge;
DROP TABLE ${stats_db_name}.alldatasetssshare purge;
DROP TABLE ${stats_db_name}.allsoftwaresshare purge;

--RIs Openess

CREATE TEMPORARY TABLE ${stats_db_name}.result_contexts as
select distinct rc.id, context.name ri_initiative from ${stats_db_name}.result_concepts rc
join ${stats_db_name}.concept on concept.id=rc.concept
join ${stats_db_name}.category on category.id=concept.category
join ${stats_db_name}.context on context.id=category.context;

CREATE TEMPORARY TABLE ${stats_db_name}.pubs_oa as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_oapubs from ${stats_db_name}.result_contexts rp
join ${stats_db_name}.publication r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by rp.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.datasets_oa as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_oadatasets from ${stats_db_name}.result_contexts rp
join ${stats_db_name}.dataset r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by rp.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.software_oa as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_oasoftware from ${stats_db_name}.result_contexts rp
join ${stats_db_name}.software r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by rp.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubs as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_allpubs from ${stats_db_name}.result_contexts rp
join ${stats_db_name}.publication r on r.id=rp.id
where cast(r.year as int)>2003
group by rp.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasets as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_alldatasets from ${stats_db_name}.result_contexts rp
join ${stats_db_name}.dataset r on r.id=rp.id
where cast(r.year as int)>2003
group by rp.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftware as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_allsoftware from ${stats_db_name}.result_contexts rp
join ${stats_db_name}.software r on r.id=rp.id
where cast(r.year as int)>2003
group by rp.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.allpubsshare as
select pubs_oa.ri_initiative, pubs_oa.no_oapubs/allpubs.no_allpubs p from ${stats_db_name}.allpubs
                        join ${stats_db_name}.pubs_oa on allpubs.ri_initiative=pubs_oa.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.alldatasetssshare as
select datasets_oa.ri_initiative, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from ${stats_db_name}.alldatasets
                             join ${stats_db_name}.datasets_oa on alldatasets.ri_initiative=datasets_oa.ri_initiative;

CREATE TEMPORARY TABLE ${stats_db_name}.allsoftwaresshare as
select software_oa.ri_initiative, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from ${stats_db_name}.allsoftware
                             join ${stats_db_name}.software_oa on allsoftware.ri_initiative=software_oa.ri_initiative;

drop table ${stats_db_name}.indi_ris_openess purge;

create table if not exists ${stats_db_name}.indi_ris_openess stored as parquet as
select allpubsshare.ri_initiative,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
	ris_openess FROM ${stats_db_name}.allpubsshare
                                left outer join (select ri_initiative,d from
    ${stats_db_name}.alldatasetssshare) tmp1
                                                on tmp1.ri_initiative=allpubsshare.ri_initiative
                                left outer join (select ri_initiative,s from
    ${stats_db_name}.allsoftwaresshare) tmp2
                                                on tmp2.ri_initiative=allpubsshare.ri_initiative;

DROP TABLE ${stats_db_name}.result_contexts purge;
DROP TABLE ${stats_db_name}.pubs_oa purge;
DROP TABLE ${stats_db_name}.datasets_oa purge;
DROP TABLE ${stats_db_name}.software_oa purge;
DROP TABLE ${stats_db_name}.allpubs purge;
DROP TABLE ${stats_db_name}.alldatasets purge;
DROP TABLE ${stats_db_name}.allsoftware purge;
DROP TABLE ${stats_db_name}.allpubsshare purge;
DROP TABLE ${stats_db_name}.alldatasetssshare purge;
DROP TABLE ${stats_db_name}.allsoftwaresshare purge;

--Funder Findability
drop table ${stats_db_name}.indi_funder_findable purge;

create table if not exists ${stats_db_name}.indi_funder_findable stored as parquet as
with result_findable as
        (select p.funder funder, count(distinct rp.id) no_result_findable from ${stats_db_name}.result_projects rp
    join ${stats_db_name}.publication r on r.id=rp.id
   join ${stats_db_name}.project p on p.id=rp.project
 join ${stats_db_name}.result_pids rpi on rpi.id=r.id
    where  cast(year as int)>2003
    group by p.funder),
    allresults as (select p.funder funder, count(distinct rp.id) no_allresults from ${stats_db_name}.result_projects rp
    join ${stats_db_name}.result r on r.id=rp.id
    join ${stats_db_name}.project p on p.id=rp.project
    where  cast(year as int)>2003
    group by p.funder)
select allresults.funder, result_findable.no_result_findable/allresults.no_allresults funder_findable
from allresults
         join result_findable on result_findable.funder=allresults.funder;

--RIs Findability
drop table ${stats_db_name}.indi_ris_findable purge;

create table if not exists ${stats_db_name}.indi_ris_findable stored as parquet as
with result_contexts as
(select distinct rc.id, context.name ri_initiative from ${stats_db_name}.result_concepts rc
join ${stats_db_name}.concept on concept.id=rc.concept
join ${stats_db_name}.category on category.id=concept.category
join ${stats_db_name}.context on context.id=category.context),
result_findable as
        (select rc.ri_initiative ri_initiative, count(distinct rc.id) no_result_findable from result_contexts rc
    join ${stats_db_name}.result r on r.id=rc.id
    join ${stats_db_name}.result_pids rp on rp.id=r.id
    where cast(r.year as int)>2003
    group by rc.ri_initiative),
allresults as
(select rc.ri_initiative ri_initiative, count(distinct rc.id) no_allresults from result_contexts rc
    join ${stats_db_name}.result r on r.id=rc.id
    where  cast(r.year as int)>2003
    group by rc.ri_initiative)
select allresults.ri_initiative, result_findable.no_result_findable/allresults.no_allresults ris_findable
from allresults
         join result_findable on result_findable.ri_initiative=allresults.ri_initiative;

