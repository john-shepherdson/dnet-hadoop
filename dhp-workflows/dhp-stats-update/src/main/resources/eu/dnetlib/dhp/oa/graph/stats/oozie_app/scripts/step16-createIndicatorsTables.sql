-- Sprint 1 ----
create table if not exists indi_pub_green_oa stored as parquet as
select distinct p.id, coalesce(green_oa, 0) as green_oa
from publication p
         left outer join (
    select p.id, 1 as green_oa
    from publication p
             join result_instance ri on ri.id = p.id
             join datasource on datasource.id = ri.hostedby
    where datasource.type like '%Repository%'
      and (ri.accessright = 'Open Access'
        or ri.accessright = 'Embargo' or ri.accessright = 'Open Source')) tmp
                         on p.id= tmp.id;

ANALYZE TABLE indi_pub_green_oa COMPUTE STATISTICS;

create table if not exists indi_pub_grey_lit stored as parquet as
select distinct p.id, coalesce(grey_lit, 0) as grey_lit
from publication p
         left outer join (
    select p.id, 1 as grey_lit
    from publication p
             join result_classifications rt on rt.id = p.id
    where rt.type not in ('Article','Part of book or chapter of book','Book','Doctoral thesis','Master thesis','Data Paper', 'Thesis', 'Bachelor thesis', 'Conference object') and
        not exists (select 1 from result_classifications rc where type ='Other literature type'
                                                              and rc.id=p.id)) tmp on p.id=tmp.id;

ANALYZE TABLE indi_pub_grey_lit COMPUTE STATISTICS;

create table if not exists indi_pub_doi_from_crossref stored as parquet as
select distinct p.id, coalesce(doi_from_crossref, 0) as doi_from_crossref
from publication p
         left outer join
     (select ri.id, 1 as doi_from_crossref from result_instance ri
                                                    join datasource d on d.id = ri.collectedfrom
      where pidtype='Digital Object Identifier' and d.name ='Crossref') tmp
     on tmp.id=p.id;

ANALYZE TABLE indi_pub_doi_from_crossref COMPUTE STATISTICS;

-- Sprint 2 ----
create table if not exists indi_result_has_cc_licence stored as parquet as
select distinct r.id, (case when lic='' or lic is null then 0 else 1 end) as has_cc_license
from result r
         left outer join (select r.id, license.type as lic from result r
                                                                    join result_licenses as license on license.id = r.id
                          where lower(license.type) LIKE '%creativecommons.org%' OR lower(license.type) LIKE '%cc-%') tmp
                         on r.id= tmp.id;

ANALYZE TABLE indi_result_has_cc_licence COMPUTE STATISTICS;

create table if not exists indi_result_has_cc_licence_url stored as parquet as
select distinct r.id, case when lic_host='' or lic_host is null then 0 else 1 end as has_cc_license_url
from result r
         left outer join (select r.id, lower(parse_url(license.type, "HOST")) as lic_host
                          from result r
                                   join result_licenses as license on license.id = r.id
                          WHERE lower(parse_url(license.type, "HOST")) = "creativecommons.org") tmp
                         on r.id= tmp.id;

ANALYZE TABLE indi_result_has_cc_licence_url COMPUTE STATISTICS;

create table if not exists indi_pub_has_abstract stored as parquet as
select distinct publication.id, cast(coalesce(abstract, true) as int) has_abstract
from publication;

ANALYZE TABLE indi_pub_has_abstract COMPUTE STATISTICS;

create table if not exists indi_result_with_orcid stored as parquet as
select distinct r.id, coalesce(has_orcid, 0) as has_orcid
from result r
         left outer join (select id, 1 as has_orcid from result_orcid) tmp
                         on r.id= tmp.id;

ANALYZE TABLE indi_result_with_orcid COMPUTE STATISTICS;

---- Sprint 3 ----
create table if not exists indi_funded_result_with_fundref stored as parquet as
select distinct r.result as id, coalesce(fundref, 0) as fundref
from project_results r
         left outer join (select distinct result, 1 as fundref from project_results
                          where provenance='Harvested') tmp
                         on r.result= tmp.result;

ANALYZE TABLE indi_funded_result_with_fundref COMPUTE STATISTICS;

-- create table indi_result_org_collab stored as parquet as
-- select o1.organization org1, o2.organization org2, count(distinct o1.id) as collaborations
-- from result_organization as o1
--          join result_organization as o2 on o1.id=o2.id and o1.organization!=o2.organization
-- group by o1.organization, o2.organization;
--
-- compute stats indi_result_org_collab;
--
create TEMPORARY TABLE tmp AS SELECT ro.organization organization, ro.id, o.name from result_organization ro
join organization o on o.id=ro.organization where o.name is not null;

create table if not exists indi_result_org_collab stored as parquet as
select o1.organization org1, o1.name org1name1, o2.organization org2, o2.name org2name2, count(o1.id) as collaborations
from tmp as o1
join tmp as o2 where o1.id=o2.id and o1.organization!=o2.organization and o1.name!=o2.name
group by o1.organization, o2.organization, o1.name, o2.name;

drop table tmp purge;

ANALYZE TABLE indi_result_org_collab COMPUTE STATISTICS;

create TEMPORARY TABLE tmp AS
select distinct ro.organization organization, ro.id, o.name, o.country from result_organization ro
join organization o on o.id=ro.organization where country <> 'UNKNOWN'  and o.name is not null;

create table if not exists indi_result_org_country_collab stored as parquet as
select o1.organization org1,o1.name org1name1, o2.country country2, count(o1.id) as collaborations
from tmp as o1 join tmp as o2 on o1.id=o2.id
where o1.id=o2.id and o1.country!=o2.country
group by o1.organization, o1.id, o1.name, o2.country;

drop table tmp purge;

ANALYZE TABLE indi_result_org_country_collab COMPUTE STATISTICS;

create TEMPORARY TABLE tmp AS
select o.id organization, o.name, ro.project as project  from organization o
        join organization_projects ro on o.id=ro.id  where o.name is not null;

create table if not exists indi_project_collab_org stored as parquet as
select o1.organization org1,o1.name orgname1, o2.organization org2, o2.name orgname2, count(distinct o1.project) as collaborations
from tmp as o1
         join tmp as o2 on o1.project=o2.project
where o1.organization<>o2.organization and o1.name<>o2.name
group by o1.name,o2.name, o1.organization, o2.organization;

drop table tmp purge;

ANALYZE TABLE indi_project_collab_org COMPUTE STATISTICS;

create TEMPORARY TABLE tmp AS
select o.id organization, o.name, o.country , ro.project as project  from organization o
        join organization_projects ro on o.id=ro.id
        and o.country <> 'UNKNOWN' and o.name is not null;

create table if not exists indi_project_collab_org_country stored as parquet as
select o1.organization org1,o1.name org1name, o2.country country2, count(distinct o1.project) as collaborations
from tmp as o1
         join tmp as o2 on o1.project=o2.project
where o1.organization<>o2.organization and o1.country<>o2.country
group by o1.organization, o2.country, o1.name;

drop table tmp purge;

ANALYZE TABLE indi_project_collab_org_country COMPUTE STATISTICS;

create table if not exists indi_funder_country_collab stored as parquet as
    with tmp as (select funder, project, country from organization_projects op
        join organization o on o.id=op.id
        join project p on p.id=op.project
        where country <> 'UNKNOWN')
select f1.funder, f1.country as country1, f2.country as country2, count(distinct f1.project) as collaborations
from tmp as f1
         join tmp as f2 on f1.project=f2.project
where f1.country<>f2.country
group by f1.funder, f2.country, f1.country;

ANALYZE TABLE indi_funder_country_collab COMPUTE STATISTICS;

create TEMPORARY TABLE tmp AS
select distinct country, ro.id as result  from organization o
        join result_organization ro on o.id=ro.organization
        where country <> 'UNKNOWN' and o.name is not null;

create table if not exists indi_result_country_collab stored as parquet as
select o1.country country1, o2.country country2, count(o1.result) as collaborations
from tmp as o1
         join tmp as o2 on o1.result=o2.result
where o1.country<>o2.country
group by o1.country, o2.country;

drop table tmp purge;

ANALYZE TABLE indi_result_country_collab COMPUTE STATISTICS;

---- Sprint 4 ----
create table if not exists indi_pub_diamond stored as parquet as
select distinct pd.id, coalesce(in_diamond_journal, 0) as in_diamond_journal
from publication_datasources pd
         left outer join (
    select pd.id, 1 as in_diamond_journal from publication_datasources pd
                                                   join datasource d on d.id=pd.datasource
                                                   join STATS_EXT.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
                                                                                 and (ps.journal_is_in_doaj=true or ps.journal_is_oa=true) and ps.has_apc=false) tmp
                         on pd.id=tmp.id;

ANALYZE TABLE indi_pub_diamond COMPUTE STATISTICS;

create table if not exists indi_pub_in_transformative stored as parquet as
select distinct pd.id, coalesce(is_transformative, 0) as is_transformative
from publication pd
         left outer join (
    select  pd.id, 1 as is_transformative from publication_datasources pd
                                                   join datasource d on d.id=pd.datasource
                                                   join STATS_EXT.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
                                                                                 and ps.is_transformative_journal=true) tmp
                         on pd.id=tmp.id;

ANALYZE TABLE indi_pub_in_transformative COMPUTE STATISTICS;

create table if not exists indi_pub_closed_other_open stored as parquet as
select distinct ri.id, coalesce(pub_closed_other_open, 0) as pub_closed_other_open from result_instance ri
                                                                                            left outer join
                                                                                        (select ri.id, 1 as pub_closed_other_open from result_instance ri
                                                                                                                                           join publication p on p.id=ri.id
                                                                                                                                           join datasource d on ri.hostedby=d.id
                                                                                         where d.type like '%Journal%' and ri.accessright='Closed Access' and
                                                                                             (p.bestlicence='Open Access' or p.bestlicence='Open Source')) tmp
                                                                                        on tmp.id=ri.id;

ANALYZE TABLE indi_pub_closed_other_open COMPUTE STATISTICS;

---- Sprint 5 ----
create table if not exists indi_result_no_of_copies stored as parquet as
select id, count(id) as number_of_copies from result_instance group by id;

ANALYZE TABLE indi_result_no_of_copies COMPUTE STATISTICS;

---- Sprint 6 ----
create table if not exists indi_pub_downloads stored as parquet as
SELECT result_id, sum(downloads) no_downloads from openaire_prod_usage_stats.usage_stats
                                                      join publication on result_id=id
where downloads>0
GROUP BY result_id
order by no_downloads desc;

ANALYZE TABLE indi_pub_downloads COMPUTE STATISTICS;

create table if not exists indi_pub_downloads_datasource stored as parquet as
SELECT result_id, repository_id, sum(downloads) no_downloads from openaire_prod_usage_stats.usage_stats
                                                                     join publication on result_id=id
where downloads>0
GROUP BY result_id, repository_id
order by result_id;

ANALYZE TABLE indi_pub_downloads_datasource COMPUTE STATISTICS;

create table if not exists indi_pub_downloads_year stored as parquet as
SELECT result_id, substring(us.`date`, 1,4) as `year`, sum(downloads) no_downloads
from openaire_prod_usage_stats.usage_stats us
join publication on result_id=id where downloads>0
GROUP BY result_id, substring(us.`date`, 1,4);

ANALYZE TABLE indi_pub_downloads_year COMPUTE STATISTICS;

create table if not exists indi_pub_downloads_datasource_year stored as parquet as
SELECT result_id, substring(us.`date`, 1,4) as `year`, repository_id, sum(downloads) no_downloads from openaire_prod_usage_stats.usage_stats us
join publication on result_id=id
where downloads>0
GROUP BY result_id, repository_id, substring(us.`date`, 1,4);

ANALYZE TABLE indi_pub_downloads_datasource_year COMPUTE STATISTICS;

---- Sprint 7 ----
create table if not exists indi_pub_gold_oa stored as parquet as
    WITH gold_oa AS ( SELECT
        issn_l,
        journal_is_in_doaj,
        journal_is_oa,
        issn_1 as issn
        FROM
        STATS_EXT.oa_journals
        WHERE
        issn_1 != ""
        UNION
        ALL SELECT
        issn_l,
        journal_is_in_doaj,
        journal_is_oa,
        issn_2 as issn
        FROM
        STATS_EXT.oa_journals
        WHERE
        issn_2 != "" ),  issn AS ( SELECT
                                   *
                                   FROM
( SELECT
                                   id,
                                   issn_printed as issn
                                   FROM
                                   datasource
                                   WHERE
                                   issn_printed IS NOT NULL
                                   UNION ALL
                                   SELECT
                                   id,
                                   issn_online as issn
                                   FROM
                                   datasource
                                   WHERE
                                   issn_online IS NOT NULL or id like '%doajarticles%') as issn
    WHERE
    LENGTH(issn) > 7)
SELECT
    DISTINCT pd.id, coalesce(is_gold, 0) as is_gold
FROM
    publication_datasources pd
        left outer join(
        select pd.id, 1 as is_gold FROM publication_datasources pd
                                            JOIN issn on issn.id=pd.datasource
                                            JOIN gold_oa  on issn.issn = gold_oa.issn) tmp
                       on pd.id=tmp.id;

ANALYZE TABLE indi_pub_gold_oa COMPUTE STATISTICS;

create table if not exists indi_pub_hybrid_oa_with_cc stored as parquet as
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
                FROM datasource
                WHERE issn_printed IS NOT NULL
                UNION ALL
                SELECT id,issn_online as issn
                FROM datasource
                WHERE issn_online IS NOT NULL ) as issn
    WHERE LENGTH(issn) > 7)
SELECT DISTINCT pd.id, coalesce(is_hybrid_oa, 0) as is_hybrid_oa
FROM publication_datasources pd
         LEFT OUTER JOIN (
    SELECT pd.id, 1 as is_hybrid_oa from publication_datasources pd
                                             JOIN datasource d on d.id=pd.datasource
                                             JOIN issn on issn.id=pd.datasource
                                             JOIN hybrid_oa ON issn.issn = hybrid_oa.issn
                                             JOIN indi_result_has_cc_licence cc on pd.id=cc.id
                                             JOIN indi_pub_gold_oa ga on pd.id=ga.id
    where cc.has_cc_license=1 and ga.is_gold=0) tmp on pd.id=tmp.id;

ANALYZE TABLE indi_pub_hybrid_oa_with_cc COMPUTE STATISTICS;

create table if not exists indi_pub_hybrid stored as parquet as
    WITH gold_oa AS ( SELECT
        issn_l,
        journal_is_in_doaj,
        journal_is_oa,
        issn_1 as issn,
        has_apc
        FROM
        STATS_EXT.oa_journals
        WHERE
        issn_1 != ""
        UNION
        ALL SELECT
        issn_l,
        journal_is_in_doaj,
        journal_is_oa,
        issn_2 as issn,
        has_apc
        FROM
        STATS_EXT.oa_journals
        WHERE
        issn_2 != "" ),  issn AS ( SELECT
                                   *
                                   FROM
( SELECT
                                   id,
                                   issn_printed as issn
                                   FROM
                                   datasource
                                   WHERE
                                   issn_printed IS NOT NULL
                                   UNION ALL
                                   SELECT
                                   id,
                                   issn_online as issn
                                   FROM
                                   datasource
                                   WHERE
                                   issn_online IS NOT NULL or id like '%doajarticles%') as issn
    WHERE
    LENGTH(issn) > 7)
select distinct pd.id, coalesce(is_hybrid, 0) as is_hybrid
from publication_datasources pd
         left outer join (
    select pd.id, 1 as is_hybrid from publication_datasources pd
                                          join datasource d on d.id=pd.datasource
                                          join issn on issn.id=pd.datasource
                                          join gold_oa on issn.issn=gold_oa.issn
    where (gold_oa.journal_is_in_doaj=false or gold_oa.journal_is_oa=false))tmp
                         on pd.id=tmp.id;

ANALYZE TABLE indi_pub_hybrid COMPUTE STATISTICS;

create table if not exists indi_org_fairness stored as parquet as
--return results with PIDs, and rich metadata group by organization
    with result_fair as
        (select ro.organization organization, count(distinct ro.id) no_result_fair from result_organization ro
    join result r on r.id=ro.id
--join result_pids rp on r.id=rp.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
    group by ro.organization),
--return all results group by organization
    allresults as (select organization, count(distinct ro.id) no_allresults from result_organization ro
    join result r on r.id=ro.id
    where  cast(year as int)>2003
    group by organization)
--return results_fair/all_results
select allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
from allresults
         join result_fair on result_fair.organization=allresults.organization;

ANALYZE TABLE indi_org_fairness COMPUTE STATISTICS;

create table if not exists indi_org_fairness_pub_pr stored as parquet as
    with result_fair as
        (select ro.organization organization, count(distinct ro.id) no_result_fair
    from result_organization ro
    join publication p on p.id=ro.id
    join indi_pub_doi_from_crossref dc on dc.id=p.id
    join indi_pub_grey_lit gl on gl.id=p.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null)
    and (authors>0) and cast(year as int)>2003 and dc.doi_from_crossref=1 and gl.grey_lit=0
    group by ro.organization),
    allresults as (select organization, count(distinct ro.id) no_allresults from result_organization ro
    join publication p on p.id=ro.id
    where cast(year as int)>2003
    group by organization)
--return results_fair/all_results
select allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
from allresults
         join result_fair on result_fair.organization=allresults.organization;

ANALYZE TABLE indi_org_fairness_pub_pr COMPUTE STATISTICS;

CREATE TEMPORARY table result_fair as
    select year, ro.organization organization, count(distinct ro.id) no_result_fair from result_organization ro
    join result p on p.id=ro.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and cast(year as int)>2003
    group by ro.organization, year;

CREATE TEMPORARY TABLE allresults as select year, organization, count(distinct ro.id) no_allresults from result_organization ro
    join result p on p.id=ro.id
    where cast(year as int)>2003
    group by organization, year;

create table if not exists indi_org_fairness_pub_year stored as parquet as
select allresults.year, allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
from allresults
         join result_fair on result_fair.organization=allresults.organization and result_fair.year=allresults.year;

DROP table result_fair purge;
DROP table allresults purge;

ANALYZE TABLE indi_org_fairness_pub_year COMPUTE STATISTICS;

CREATE TEMPORARY TABLE result_fair as
    select ro.organization organization, count(distinct ro.id) no_result_fair
     from result_organization ro
              join result p on p.id=ro.id
     where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null)
       and (authors>0) and cast(year as int)>2003
     group by ro.organization;

CREATE TEMPORARY TABLE allresults as
    select organization, count(distinct ro.id) no_allresults from result_organization ro
    join result p on p.id=ro.id
    where cast(year as int)>2003
    group by organization;

create table if not exists indi_org_fairness_pub as
select allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
from allresults join result_fair on result_fair.organization=allresults.organization;

DROP table result_fair purge;
DROP table allresults purge;

ANALYZE TABLE indi_org_fairness_pub COMPUTE STATISTICS;

CREATE TEMPORARY TABLE result_fair as
    select year, ro.organization organization, count(distinct ro.id) no_result_fair from result_organization ro
    join result r on r.id=ro.id
    join result_pids rp on r.id=rp.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
    group by ro.organization, year;

CREATE TEMPORARY TABLE allresults as
    select year, organization, count(distinct ro.id) no_allresults from result_organization ro
    join result r on r.id=ro.id
    where  cast(year as int)>2003
    group by organization, year;

create table if not exists indi_org_fairness_year stored as parquet as
    select allresults.year, allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
    from allresults
    join result_fair on result_fair.organization=allresults.organization and result_fair.year=allresults.year;

DROP table result_fair purge;
DROP table allresults purge;

ANALYZE TABLE indi_org_fairness_year COMPUTE STATISTICS;

CREATE TEMPORARY TABLE result_with_pid as
    select year, ro.organization organization, count(distinct rp.id) no_result_with_pid from result_organization ro
    join result_pids rp on rp.id=ro.id
    join result r on r.id=rp.id
    where cast(year as int) >2003
    group by ro.organization, year;

CREATE TEMPORARY TABLE allresults as
    select year, organization, count(distinct ro.id) no_allresults from result_organization ro
    join result r on r.id=ro.id
    where cast(year as int) >2003
    group by organization, year;

create table if not exists indi_org_findable_year stored as parquet as
select allresults.year, allresults.organization, result_with_pid.no_result_with_pid/allresults.no_allresults org_findable
from allresults
         join result_with_pid on result_with_pid.organization=allresults.organization and result_with_pid.year=allresults.year;

DROP table result_with_pid purge;
DROP table allresults purge;

ANALYZE TABLE indi_org_findable_year COMPUTE STATISTICS;

CREATE TEMPORARY TABLE result_with_pid as
select ro.organization organization, count(distinct rp.id) no_result_with_pid from result_organization ro
    join result_pids rp on rp.id=ro.id
    join result r on r.id=rp.id
    where cast(year as int) >2003
    group by ro.organization;

CREATE TEMPORARY TABLE allresults as
select organization, count(distinct ro.id) no_allresults from result_organization ro
    join result r on r.id=ro.id
    where cast(year as int) >2003
    group by organization;

create table if not exists indi_org_findable stored as parquet as
select allresults.organization, result_with_pid.no_result_with_pid/allresults.no_allresults org_findable
from allresults
         join result_with_pid on result_with_pid.organization=allresults.organization;

DROP table result_with_pid purge;
DROP table allresults purge;

ANALYZE TABLE indi_org_findable COMPUTE STATISTICS;

CREATE TEMPORARY TABLE pubs_oa as
SELECT ro.organization, count(distinct r.id) no_oapubs FROM publication r
    join result_organization ro on ro.id=r.id
    join result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE datasets_oa as
SELECT ro.organization, count(distinct r.id) no_oadatasets FROM dataset r
    join result_organization ro on ro.id=r.id
    join result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE software_oa as
SELECT ro.organization, count(distinct r.id) no_oasoftware FROM software r
    join result_organization ro on ro.id=r.id
    join result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE allpubs as
SELECT ro.organization organization, count(ro.id) no_allpubs FROM result_organization ro
    join publication ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE alldatasets as
SELECT ro.organization organization, count(ro.id) no_alldatasets FROM result_organization ro
    join dataset ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE allsoftware as
SELECT ro.organization organization, count(ro.id) no_allsoftware FROM result_organization ro
    join software ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization;

CREATE TEMPORARY TABLE allpubsshare as
select pubs_oa.organization, pubs_oa.no_oapubs/allpubs.no_allpubs p from allpubs
                        join pubs_oa on allpubs.organization=pubs_oa.organization;

CREATE TEMPORARY TABLE alldatasetssshare as
select datasets_oa.organization, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from alldatasets
                             join datasets_oa on alldatasets.organization=datasets_oa.organization;

CREATE TEMPORARY TABLE allsoftwaresshare as
select software_oa.organization, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from allsoftware
                             join software_oa on allsoftware.organization=software_oa.organization;

create table if not exists indi_org_openess stored as parquet as
select allpubsshare.organization,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           org_openess FROM allpubsshare
                                left outer join (select organization,d from
    alldatasetssshare) tmp1
                                                on tmp1.organization=allpubsshare.organization
                                left outer join (select organization,s from
    allsoftwaresshare) tmp2
                                                on tmp2.organization=allpubsshare.organization;

DROP TABLE pubs_oa purge;
DROP TABLE datasets_oa purge;
DROP TABLE software_oa purge;
DROP TABLE allpubs purge;
DROP TABLE alldatasets purge;
DROP TABLE allsoftware purge;
DROP TABLE allpubsshare purge;
DROP TABLE alldatasetssshare purge;
DROP TABLE allsoftwaresshare purge;

ANALYZE TABLE indi_org_openess COMPUTE STATISTICS;

CREATE TEMPORARY TABLE pubs_oa AS
SELECT r.year, ro.organization, count(distinct r.id) no_oapubs FROM publication r
    join result_organization ro on ro.id=r.id
    join result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization,r.year;

CREATE TEMPORARY TABLE datasets_oa AS
SELECT r.year,ro.organization, count(distinct r.id) no_oadatasets FROM dataset r
    join result_organization ro on ro.id=r.id
    join result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization, r.year;

CREATE TEMPORARY TABLE software_oa AS
SELECT r.year,ro.organization, count(distinct r.id) no_oasoftware FROM software r
    join result_organization ro on ro.id=r.id
    join result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization, r.year;

CREATE TEMPORARY TABLE allpubs as
SELECT p.year,ro.organization organization, count(ro.id) no_allpubs FROM result_organization ro
    join publication p on p.id=ro.id where cast(p.year as int)>2003
    group by ro.organization, p.year;

CREATE TEMPORARY TABLE alldatasets as
SELECT d.year, ro.organization organization, count(ro.id) no_alldatasets FROM result_organization ro
    join dataset d on d.id=ro.id where cast(d.year as int)>2003
    group by ro.organization, d.year;

CREATE TEMPORARY TABLE allsoftware as
SELECT s.year,ro.organization organization, count(ro.id) no_allsoftware FROM result_organization ro
    join software s on s.id=ro.id where cast(s.year as int)>2003
    group by ro.organization, s.year;

CREATE TEMPORARY TABLE allpubsshare as
select allpubs.year, pubs_oa.organization, pubs_oa.no_oapubs/allpubs.no_allpubs p from allpubs
                        join pubs_oa on allpubs.organization=pubs_oa.organization where cast(allpubs.year as INT)=cast(pubs_oa.year as int);

CREATE TEMPORARY TABLE alldatasetssshare as
select alldatasets.year, datasets_oa.organization, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from alldatasets
                             join datasets_oa on alldatasets.organization=datasets_oa.organization where cast(alldatasets.year as INT)=cast(datasets_oa.year as int);

CREATE TEMPORARY TABLE allsoftwaresshare as
select allsoftware.year, software_oa.organization, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from allsoftware
                             join software_oa on allsoftware.organization=software_oa.organization where cast(allsoftware.year as INT)=cast(software_oa.year as int);


create table if not exists indi_org_openess_year stored as parquet as
select allpubsshare.year, allpubsshare.organization,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           org_openess FROM allpubsshare
                                left outer join (select year, organization,d from
    alldatasetssshare) tmp1
                                                on tmp1.organization=allpubsshare.organization and tmp1.year=allpubsshare.year
                                left outer join (select year, organization,s from
    allsoftwaresshare) tmp2
                                                on tmp2.organization=allpubsshare.organization and tmp2.year=allpubsshare.year;

DROP TABLE pubs_oa purge;
DROP TABLE datasets_oa purge;
DROP TABLE software_oa purge;
DROP TABLE allpubs purge;
DROP TABLE alldatasets purge;
DROP TABLE allsoftware purge;
DROP TABLE allpubsshare purge;
DROP TABLE alldatasetssshare purge;
DROP TABLE allsoftwaresshare purge;

ANALYZE TABLE indi_org_openess_year COMPUTE STATISTICS;

create table if not exists indi_pub_has_preprint stored as parquet as
select distinct p.id, coalesce(has_preprint, 0) as has_preprint
from publication_classifications p
         left outer join (
    select p.id, 1 as has_preprint
    from publication_classifications p
    where p.type='Preprint') tmp
                         on p.id= tmp.id;

ANALYZE TABLE indi_pub_has_preprint COMPUTE STATISTICS;

create table if not exists indi_pub_in_subscribed stored as parquet as
select distinct p.id, coalesce(is_subscription, 0) as is_subscription
from publication p
         left outer join(
    select  p.id, 1 as is_subscription from publication p
                                                join indi_pub_gold_oa g on p.id=g.id
                                                join indi_pub_hybrid h on p.id=h.id
                                                join indi_pub_in_transformative t on p.id=t.id
    where g.is_gold=0 and h.is_hybrid=0 and t.is_transformative=0) tmp
                        on p.id=tmp.id;

ANALYZE TABLE indi_pub_in_subscribed COMPUTE STATISTICS;

create table if not exists indi_result_with_pid as
select distinct p.id, coalesce(result_with_pid, 0) as result_with_pid
from result p
         left outer join (
    select p.id, 1 as result_with_pid
    from result_pids p) tmp
                         on p.id= tmp.id;

ANALYZE TABLE indi_result_with_pid COMPUTE STATISTICS;

CREATE TEMPORARY TABLE pub_fos_totals as
select rf.id, count(distinct lvl3) totals from result_fos rf
group by rf.id;

create table if not exists indi_pub_interdisciplinarity as
select distinct p.id as id, coalesce(is_interdisciplinary, 0)
as is_interdisciplinary
from pub_fos_totals p
left outer join (
select pub_fos_totals.id, 1 as is_interdisciplinary from pub_fos_totals
where totals>1) tmp on p.id=tmp.id;

drop table pub_fos_totals purge;

ANALYZE TABLE indi_pub_interdisciplinarity COMPUTE STATISTICS;

create table if not exists indi_pub_bronze_oa stored as parquet as
select distinct p.id, coalesce(is_bronze_oa,0) as is_bronze_oa
from publication p
left outer join
(select p.id, 1 as is_bronze_oa from publication p
join indi_result_has_cc_licence cc on cc.id=p.id
join indi_pub_gold_oa ga on ga.id=p.id
where cc.has_cc_license=0 and ga.is_gold=0) tmp on tmp.id=p.id;

-- create table if not exists indi_pub_bronze_oa stored as parquet as
--    WITH hybrid_oa AS (
--        SELECT issn_l, journal_is_in_doaj, journal_is_oa, issn_print as issn
--        FROM STATS_EXT.plan_s_jn
--        WHERE issn_print != ""
--        UNION ALL
--        SELECT issn_l, journal_is_in_doaj, journal_is_oa, issn_online as issn
--        FROM STATS_EXT.plan_s_jn
--        WHERE issn_online != "" and (journal_is_in_doaj = FALSE OR journal_is_oa = FALSE)),
--    issn AS (
--                SELECT *
--                FROM (
--                SELECT id, issn_printed as issn
--                FROM datasource
--                WHERE issn_printed IS NOT NULL
--                UNION ALL
--                SELECT id,issn_online as issn
--                FROM datasource
--                WHERE issn_online IS NOT NULL ) as issn
--    WHERE LENGTH(issn) > 7)
--SELECT DISTINCT pd.id, coalesce(is_bronze_oa, 0) as is_bronze_oa
--FROM publication_datasources pd
--         LEFT OUTER JOIN (
--    SELECT pd.id, 1 as is_bronze_oa from publication_datasources pd
--                                             JOIN datasource d on d.id=pd.datasource
--                                             JOIN issn on issn.id=pd.datasource
--                                             JOIN hybrid_oa ON issn.issn = hybrid_oa.issn
--                                             JOIN indi_result_has_cc_licence cc on pd.id=cc.id
--                                             JOIN indi_pub_gold_oa ga on pd.id=ga.id
--                                             JOIN indi_pub_hybrid_oa_with_cc hy on hy.id=pd.id
--    where cc.has_cc_license=0 and ga.is_gold=0 and hy.is_hybrid_oa=0) tmp on pd.id=tmp.id;

ANALYZE TABLE indi_pub_bronze_oa COMPUTE STATISTICS;