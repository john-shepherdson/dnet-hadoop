-- Sprint 1 ----
drop table if exists ${stats_db_name}.indi_pub_green_oa purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_green_oa stored as parquet as
select distinct p.id, coalesce(green_oa, 0) as green_oa
from ${stats_db_name}.publication p
left outer join (
    select p.id, 1 as green_oa
    from ${stats_db_name}.publication p
    join ${stats_db_name}.result_instance ri on ri.id = p.id
    join ${stats_db_name}.datasource on datasource.id = ri.hostedby
    where datasource.type like '%Repository%' and (ri.accessright = 'Open Access' or ri.accessright = 'Embargo' or ri.accessright = 'Open Source') and datasource.name!='Other') tmp on p.id= tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_grey_lit purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_grey_lit stored as parquet as
select distinct p.id, coalesce(grey_lit, 0) as grey_lit
from ${stats_db_name}.publication p
left outer join (
    select p.id, 1 as grey_lit
    from ${stats_db_name}.publication p
    join ${stats_db_name}.result_classifications rt on rt.id = p.id
    where rt.type not in ('Article','Part of book or chapter of book','Book','Doctoral thesis','Master thesis','Data Paper', 'Thesis', 'Bachelor thesis', 'Conference object')
        and not exists (select 1 from ${stats_db_name}.result_classifications rc where type ='Other literature type' and rc.id=p.id)) tmp on p.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_doi_from_crossref purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_doi_from_crossref stored as parquet as
select distinct p.id, coalesce(doi_from_crossref, 0) as doi_from_crossref
from ${stats_db_name}.publication p
left outer join (
    select ri.id, 1 as doi_from_crossref from ${stats_db_name}.result_instance ri
    join ${stats_db_name}.datasource d on d.id = ri.collectedfrom
    where pidtype='Digital Object Identifier' and d.name ='Crossref') tmp on tmp.id=p.id; /*EOS*/

-- Sprint 2 ----
drop table if exists ${stats_db_name}.indi_result_has_cc_licence purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_result_has_cc_licence stored as parquet as
select distinct r.id, (case when lic='' or lic is null then 0 else 1 end) as has_cc_license
from ${stats_db_name}.result r
left outer join (
    select r.id, license.type as lic from ${stats_db_name}.result r
    join ${stats_db_name}.result_licenses as license on license.id = r.id
    where lower(license.type) LIKE '%creativecommons.org%' OR lower(license.type) LIKE '%cc %') tmp on r.id= tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_has_cc_licence_url purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_result_has_cc_licence_url stored as parquet as
select distinct r.id, case when lic_host='' or lic_host is null then 0 else 1 end as has_cc_license_url
from ${stats_db_name}.result r
left outer join (
    select r.id, lower(parse_url(license.type, "HOST")) as lic_host
    from ${stats_db_name}.result r
    join ${stats_db_name}.result_licenses as license on license.id = r.id
    WHERE lower(parse_url(license.type, "HOST")) = "creativecommons.org") tmp on r.id= tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_has_abstract purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_has_abstract stored as parquet as
select distinct publication.id, cast(coalesce(abstract, true) as int) has_abstract
from ${stats_db_name}.publication; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_with_orcid purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_result_with_orcid stored as parquet as
select distinct r.id, coalesce(has_orcid, 0) as has_orcid
from ${stats_db_name}.result r
left outer join (
    select id, 1 as has_orcid from ${stats_db_name}.result_orcid) tmp on r.id= tmp.id; /*EOS*/


---- Sprint 3 ----
drop table if exists ${stats_db_name}.indi_funded_result_with_fundref purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_funded_result_with_fundref stored as parquet as
select distinct r.result as id, coalesce(fundref, 0) as fundref
from ${stats_db_name}.project_results r
left outer join (
    select distinct result, 1 as fundref from ${stats_db_name}.project_results where provenance='Harvested') tmp on r.result= tmp.result; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_org_collab purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_result_org_collab stored as parquet as
    WITH tmp AS (
        SELECT ro.organization organization, ro.id, o.name
        from ${stats_db_name}.result_organization ro
        join ${stats_db_name}.organization o on o.id=ro.organization where o.name is not null)
    select o1.organization org1, o1.name org1name1, o2.organization org2, o2.name org2name2, count(o1.id) as collaborations
    from tmp as o1
    join tmp as o2 where o1.id=o2.id and o1.organization!=o2.organization and o1.name!=o2.name
    group by o1.organization, o2.organization, o1.name, o2.name; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_org_country_collab purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_result_org_country_collab stored as parquet as
    WITH tmp AS (
        select distinct ro.organization organization, ro.id, o.name, o.country
        from ${stats_db_name}.result_organization ro
        join ${stats_db_name}.organization o on o.id=ro.organization
        where country <> 'UNKNOWN'  and o.name is not null)
    select o1.organization org1,o1.name org1name1, o2.country country2, count(o1.id) as collaborations
    from tmp as o1 join tmp as o2 on o1.id=o2.id
    where o1.id=o2.id and o1.country!=o2.country
    group by o1.organization, o1.id, o1.name, o2.country; /*EOS*/

drop table if exists ${stats_db_name}.indi_project_collab_org purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_project_collab_org stored as parquet as
    WITH tmp AS (
        select o.id organization, o.name, ro.project as project
        from ${stats_db_name}.organization o
        join ${stats_db_name}.organization_projects ro on o.id=ro.id  where o.name is not null)
    select o1.organization org1,o1.name orgname1, o2.organization org2, o2.name orgname2, count(distinct o1.project) as collaborations
    from tmp as o1
    join tmp as o2 on o1.project=o2.project
    where o1.organization<>o2.organization and o1.name<>o2.name
    group by o1.name,o2.name, o1.organization, o2.organization; /*EOS*/

drop table if exists ${stats_db_name}.indi_project_collab_org_country purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_project_collab_org_country stored as parquet as
    WITH tmp AS (
        select o.id organization, o.name, o.country , ro.project as project
        from ${stats_db_name}.organization o
        join ${stats_db_name}.organization_projects ro on o.id=ro.id and o.country <> 'UNKNOWN' and o.name is not null)
    select o1.organization org1,o1.name org1name, o2.country country2, count(distinct o1.project) as collaborations
    from tmp as o1
    join tmp as o2 on o1.project=o2.project
    where o1.organization<>o2.organization and o1.country<>o2.country
    group by o1.organization, o2.country, o1.name; /*EOS*/

drop table if exists ${stats_db_name}.indi_funder_country_collab purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_funder_country_collab stored as parquet as
    with tmp as (select funder, project, country from ${stats_db_name}.organization_projects op
        join ${stats_db_name}.organization o on o.id=op.id
        join ${stats_db_name}.project p on p.id=op.project
        where country <> 'UNKNOWN')
    select f1.funder, f1.country as country1, f2.country as country2, count(distinct f1.project) as collaborations
    from tmp as f1
    join tmp as f2 on f1.project=f2.project
    where f1.country<>f2.country
    group by f1.funder, f2.country, f1.country; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_country_collab purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_result_country_collab stored as parquet as
    WITH tmp AS (
        select distinct country, ro.id as result  from ${stats_db_name}.organization o
        join ${stats_db_name}.result_organization ro on o.id=ro.organization
        where country <> 'UNKNOWN' and o.name is not null)
    select o1.country country1, o2.country country2, count(o1.result) as collaborations
    from tmp as o1
    join tmp as o2 on o1.result=o2.result
    where o1.country<>o2.country
    group by o1.country, o2.country; /*EOS*/


---- Sprint 4 ----
drop table if exists ${stats_db_name}.indi_pub_diamond purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_diamond stored as parquet as
    select distinct pd.id, coalesce(in_diamond_journal, 0) as in_diamond_journal
    from ${stats_db_name}.publication_datasources pd
    left outer join (
        select pd.id, 1 as in_diamond_journal
        from ${stats_db_name}.publication_datasources pd
        join ${stats_db_name}.datasource d on d.id=pd.datasource
        join STATS_EXT.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
            and (ps.journal_is_in_doaj=true or ps.journal_is_oa=true) and ps.has_apc=false) tmp on pd.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_in_transformative purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_in_transformative stored as parquet as
    select distinct pd.id, coalesce(is_transformative, 0) as is_transformative
    from ${stats_db_name}.publication pd
    left outer join (
        select  pd.id, 1 as is_transformative
        from ${stats_db_name}.publication_datasources pd
        join ${stats_db_name}.datasource d on d.id=pd.datasource
        join STATS_EXT.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
            and ps.is_transformative_journal=true) tmp on pd.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_closed_other_open purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_closed_other_open stored as parquet as
    select distinct ri.id, coalesce(pub_closed_other_open, 0) as pub_closed_other_open
    from ${stats_db_name}.result_instance ri
    left outer join (
        select ri.id, 1 as pub_closed_other_open
        from ${stats_db_name}.result_instance ri
        join ${stats_db_name}.publication p on p.id=ri.id
        join ${stats_db_name}.datasource d on ri.hostedby=d.id
        where d.type like '%Journal%' and ri.accessright='Closed Access' and
            (p.bestlicence='Open Access' or p.bestlicence='Open Source')) tmp on tmp.id=ri.id; /*EOS*/


---- Sprint 5 ----
drop table if exists ${stats_db_name}.indi_result_no_of_copies purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_result_no_of_copies stored as parquet as
    select id, count(id) as number_of_copies
    from ${stats_db_name}.result_instance
    group by id; /*EOS*/

---- Sprint 6 ----
drop table if exists ${stats_db_name}.indi_pub_downloads purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_downloads stored as parquet as
    SELECT result_id, sum(downloads) no_downloads
    from openaire_prod_usage_stats.usage_stats
    join ${stats_db_name}.publication on result_id=id
    where downloads>0
    GROUP BY result_id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_downloads_datasource purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_downloads_datasource stored as parquet as
    SELECT result_id, repository_id, sum(downloads) no_downloads
    from openaire_prod_usage_stats.usage_stats
    join ${stats_db_name}.publication on result_id=id
    where downloads>0
    GROUP BY result_id, repository_id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_downloads_year purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_downloads_year stored as parquet as
    SELECT result_id, cast(substring(us.`date`, 1,4) as int) as `year`, sum(downloads) no_downloads
    from openaire_prod_usage_stats.usage_stats us
    join ${stats_db_name}.publication on result_id=id where downloads>0
    GROUP BY result_id, substring(us.`date`, 1,4); /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_downloads_datasource_year purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_downloads_datasource_year stored as parquet as
    SELECT result_id, cast(substring(us.`date`, 1,4) as int) as `year`, repository_id, sum(downloads) no_downloads
    from openaire_prod_usage_stats.usage_stats us
    join ${stats_db_name}.publication on result_id=id
    where downloads>0
    GROUP BY result_id, repository_id, substring(us.`date`, 1,4); /*EOS*/


---- Sprint 7 ----
drop table if exists ${stats_db_name}.indi_pub_gold_oa purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_gold_oa stored as parquet as
    with gold_oa as (
    select distinct issn from (
            SELECT issn_l as issn from stats_ext.issn_gold_oa_dataset_v5
            UNION ALL
            SELECT issn as issn from stats_ext.issn_gold_oa_dataset_v5
            UNION ALL
            select issn from stats_ext.alljournals where journal_is_in_doaj=true or journal_is_oa=true
            UNION ALL
            select issn_l as issn from stats_ext.alljournals where journal_is_in_doaj=true or journal_is_oa=true) foo),
    dd as (
    select distinct * from (
            select id, issn_printed as issn from ${stats_db_name}.datasource d where d.id like '%doajarticles%'
            UNION ALL
            select id, issn_online as issn from ${stats_db_name}.datasource d where d.id like '%doajarticles%'
            UNION ALL
            select id, issn_printed as issn from ${stats_db_name}.datasource d join gold_oa on gold_oa.issn=d.issn_printed
            UNION ALL
            select id, issn_online as issn from ${stats_db_name}.datasource d join gold_oa on gold_oa.issn=d.issn_online) foo
    )
    SELECT DISTINCT pd.id, coalesce(is_gold, 0) as is_gold
    FROM ${stats_db_name}.publication pd
    left outer join (
            select pd.id, 1 as is_gold
            FROM ${stats_db_name}.publication_datasources pd
            left semi join dd on dd.id=pd.datasource
            union all
            select ra.id, 1 as is_gold
            from ${stats_db_name}.result_accessroute ra on ra.id = pd.id where ra.accessroute = 'gold') tmp on tmp.id=pd.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_hybrid_oa_with_cc purge; /*EOS*/
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
        JOIN ${stats_db_name}.indi_pub_gold_oa ga on pd.id=ga.id where cc.has_cc_license=1 and ga.is_gold=0) tmp on pd.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_hybrid purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_pub_hybrid stored as parquet as
select distinct p.id, coalesce(is_hybrid, 0) is_hybrid
from ${stats_db_name}.publication p
left outer join (
    select p.id, 1 as is_hybrid
    from ${stats_db_name}.publication p
    join ${stats_db_name}.result_instance ri on ri.id=p.id
    join ${stats_db_name}.datasource d on d.id=ri.hostedby
    join ${stats_db_name}.indi_pub_gold_oa indi_gold on indi_gold.id=p.id
    left outer join ${stats_db_name}.result_accessroute ra on ra.id=p.id
    where indi_gold.is_gold=0 and
          ((d.type like '%Journal%' and ri.accessright not in ('Closed Access', 'Restricted', 'Not Available') and ri.license is not null) or ra.accessroute='hybrid')) tmp on pd.i=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_fairness purge; /*EOS*/
create table if not exists ${stats_db_name}.indi_org_fairness stored as parquet as
--return results with PIDs, and rich metadata group by organization
    with result_fair as (
        select ro.organization organization, count(distinct ro.id) no_result_fair
        from ${stats_db_name}.result_organization ro
        join ${stats_db_name}.result r on r.id=ro.id
--join result_pids rp on r.id=rp.id
        where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
        group by ro.organization),
--return all results group by organization
    allresults as (
        select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
        join ${stats_db_name}.result r on r.id=ro.id
        where  cast(year as int)>2003
        group by ro.organization)
--return results_fair/all_results
    select allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
    from allresults
    join result_fair on result_fair.organization=allresults.organization; /*EOS*/

CREATE TEMPORARY VIEW result_fair as
select ro.organization organization, count(distinct ro.id) no_result_fair
    from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication p on p.id=ro.id
    join ${stats_db_name}.indi_pub_doi_from_crossref dc on dc.id=p.id
    join ${stats_db_name}.indi_pub_grey_lit gl on gl.id=p.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null)
    and (authors>0) and cast(year as int)>2003 and dc.doi_from_crossref=1 and gl.grey_lit=0
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW allresults as
select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication p on p.id=ro.id
    where cast(year as int)>2003
    group by ro.organization; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_fairness_pub_pr purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_fairness_pub_pr stored as parquet as
select ar.organization, rf.no_result_fair/ar.no_allresults org_fairness
from allresults ar
         join result_fair rf on rf.organization=ar.organization; /*EOS*/

DROP VIEW result_fair;
DROP VIEW allresults;

CREATE TEMPORARY VIEW result_fair as
    select year, ro.organization organization, count(distinct ro.id) no_result_fair from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result p on p.id=ro.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and cast(year as int)>2003
    group by ro.organization, year; /*EOS*/

CREATE TEMPORARY VIEW allresults as select year, ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result p on p.id=ro.id
    where cast(year as int)>2003
    group by ro.organization, year; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_fairness_pub_year purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_fairness_pub_year stored as parquet as
select cast(allresults.year as int) year, allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
from allresults
         join result_fair on result_fair.organization=allresults.organization and result_fair.year=allresults.year; /*EOS*/

DROP VIEW result_fair; /*EOS*/
DROP VIEW allresults; /*EOS*/

CREATE TEMPORARY VIEW result_fair as
    select ro.organization organization, count(distinct ro.id) no_result_fair
     from ${stats_db_name}.result_organization ro
              join ${stats_db_name}.result p on p.id=ro.id
     where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null)
       and (authors>0) and cast(year as int)>2003
     group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW allresults as
    select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result p on p.id=ro.id
    where cast(year as int)>2003
    group by ro.organization; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_fairness_pub purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_fairness_pub as
select ar.organization, rf.no_result_fair/ar.no_allresults org_fairness
from allresults ar join result_fair rf
on rf.organization=ar.organization; /*EOS*/

DROP VIEW result_fair; /*EOS*/
DROP VIEW allresults; /*EOS*/

CREATE TEMPORARY VIEW result_fair as
    select year, ro.organization organization, count(distinct ro.id) no_result_fair from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    join ${stats_db_name}.result_pids rp on r.id=rp.id
    where (title is not null) and (publisher is not null) and (abstract=true) and (year is not null) and (authors>0) and  cast(year as int)>2003
    group by ro.organization, year; /*EOS*/

CREATE TEMPORARY VIEW allresults as
    select year, ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    where  cast(year as int)>2003
    group by ro.organization, year; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_fairness_year purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_fairness_year stored as parquet as
    select cast(allresults.year as int) year, allresults.organization, result_fair.no_result_fair/allresults.no_allresults org_fairness
    from allresults
    join result_fair on result_fair.organization=allresults.organization and cast(result_fair.year as int)=cast(allresults.year as int); /*EOS*/

DROP VIEW result_fair; /*EOS*/
DROP VIEW allresults; /*EOS*/

CREATE TEMPORARY VIEW result_with_pid as
    select year, ro.organization, count(distinct rp.id) no_result_with_pid from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result_pids rp on rp.id=ro.id
    join ${stats_db_name}.result r on r.id=rp.id
    where cast(year as int) >2003
    group by ro.organization, year; /*EOS*/

CREATE TEMPORARY VIEW allresults as
    select year, ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    where cast(year as int) >2003
    group by ro.organization, year; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_findable_year purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_findable_year stored as parquet as
select cast(allresults.year as int) year, allresults.organization, result_with_pid.no_result_with_pid/allresults.no_allresults org_findable
from allresults
         join result_with_pid on result_with_pid.organization=allresults.organization and cast(result_with_pid.year as int)=cast(allresults.year as int); /*EOS*/

DROP VIEW result_with_pid; /*EOS*/
DROP VIEW allresults; /*EOS*/

CREATE TEMPORARY VIEW result_with_pid as
select ro.organization, count(distinct rp.id) no_result_with_pid from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result_pids rp on rp.id=ro.id
    join ${stats_db_name}.result r on r.id=rp.id
    where cast(year as int) >2003
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW allresults as
select ro.organization, count(distinct ro.id) no_allresults from ${stats_db_name}.result_organization ro
    join ${stats_db_name}.result r on r.id=ro.id
    where cast(year as int) >2003
    group by ro.organization; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_findable purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_findable stored as parquet as
select allresults.organization, result_with_pid.no_result_with_pid/allresults.no_allresults org_findable
from allresults
         join result_with_pid on result_with_pid.organization=allresults.organization; /*EOS*/

DROP VIEW result_with_pid; /*EOS*/
DROP VIEW allresults; /*EOS*/

CREATE TEMPORARY VIEW pubs_oa as
SELECT ro.organization, count(distinct r.id) no_oapubs FROM ${stats_db_name}.publication r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW datasets_oa as
SELECT ro.organization, count(distinct r.id) no_oadatasets FROM ${stats_db_name}.dataset r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW software_oa as
SELECT ro.organization, count(distinct r.id) no_oasoftware FROM ${stats_db_name}.software r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW allpubs as
SELECT ro.organization, count(ro.id) no_allpubs FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW alldatasets as
SELECT ro.organization, count(ro.id) no_alldatasets FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.dataset ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW allsoftware as
SELECT ro.organization, count(ro.id) no_allsoftware FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.software ps on ps.id=ro.id
    where cast(ps.year as int)>2003
    group by ro.organization; /*EOS*/

CREATE TEMPORARY VIEW allpubsshare as
select pubs_oa.organization, pubs_oa.no_oapubs/allpubs.no_allpubs p from allpubs
                        join pubs_oa on allpubs.organization=pubs_oa.organization; /*EOS*/

CREATE TEMPORARY VIEW alldatasetssshare as
select datasets_oa.organization, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from alldatasets
                             join datasets_oa on alldatasets.organization=datasets_oa.organization; /*EOS*/

CREATE TEMPORARY VIEW allsoftwaresshare as
select software_oa.organization, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from allsoftware
                             join software_oa on allsoftware.organization=software_oa.organization; /*EOS*/

drop table if exists ${stats_db_name}.indi_org_openess purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_openess stored as parquet as
select allpubsshare.organization,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           org_openess FROM allpubsshare
                                left outer join (select organization,d from
    alldatasetssshare) tmp1
                                                on tmp1.organization=allpubsshare.organization
                                left outer join (select organization,s from
    allsoftwaresshare) tmp2
                                                on tmp2.organization=allpubsshare.organization; /*EOS*/

DROP VIEW pubs_oa; /*EOS*/
DROP VIEW datasets_oa; /*EOS*/
DROP VIEW software_oa; /*EOS*/
DROP VIEW allpubs; /*EOS*/
DROP VIEW alldatasets; /*EOS*/
DROP VIEW allsoftware; /*EOS*/
DROP VIEW allpubsshare; /*EOS*/
DROP VIEW alldatasetssshare; /*EOS*/
DROP VIEW allsoftwaresshare; /*EOS*/

CREATE TEMPORARY VIEW pubs_oa AS
SELECT r.year, ro.organization, count(distinct r.id) no_oapubs FROM ${stats_db_name}.publication r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization,r.year; /*EOS*/

CREATE TEMPORARY VIEW datasets_oa AS
SELECT r.year,ro.organization, count(distinct r.id) no_oadatasets FROM ${stats_db_name}.dataset r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization, r.year; /*EOS*/

CREATE TEMPORARY VIEW software_oa AS
SELECT r.year,ro.organization, count(distinct r.id) no_oasoftware FROM ${stats_db_name}.software r
    join ${stats_db_name}.result_organization ro on ro.id=r.id
    join ${stats_db_name}.result_instance ri on ri.id=r.id
    where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
    and cast(r.year as int)>2003
    group by ro.organization, r.year; /*EOS*/

CREATE TEMPORARY VIEW allpubs as
SELECT p.year,ro.organization organization, count(ro.id) no_allpubs FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.publication p on p.id=ro.id where cast(p.year as int)>2003
    group by ro.organization, p.year; /*EOS*/

CREATE TEMPORARY VIEW alldatasets as
SELECT d.year, ro.organization organization, count(ro.id) no_alldatasets FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.dataset d on d.id=ro.id where cast(d.year as int)>2003
    group by ro.organization, d.year; /*EOS*/

CREATE TEMPORARY VIEW allsoftware as
SELECT s.year,ro.organization organization, count(ro.id) no_allsoftware FROM ${stats_db_name}.result_organization ro
    join ${stats_db_name}.software s on s.id=ro.id where cast(s.year as int)>2003
    group by ro.organization, s.year; /*EOS*/

CREATE TEMPORARY VIEW allpubsshare as
select allpubs.year, pubs_oa.organization, pubs_oa.no_oapubs/allpubs.no_allpubs p from allpubs
                        join pubs_oa on allpubs.organization=pubs_oa.organization where cast(allpubs.year as INT)=cast(pubs_oa.year as int); /*EOS*/

CREATE TEMPORARY VIEW alldatasetssshare as
select alldatasets.year, datasets_oa.organization, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from alldatasets
                             join datasets_oa on alldatasets.organization=datasets_oa.organization where cast(alldatasets.year as INT)=cast(datasets_oa.year as int); /*EOS*/

CREATE TEMPORARY VIEW allsoftwaresshare as
select allsoftware.year, software_oa.organization, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from allsoftware
                             join software_oa on allsoftware.organization=software_oa.organization where cast(allsoftware.year as INT)=cast(software_oa.year as int); /*EOS*/

drop table if exists ${stats_db_name}.indi_org_openess_year purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_org_openess_year stored as parquet as
select cast(allpubsshare.year as int) year, allpubsshare.organization,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           org_openess FROM allpubsshare
                                left outer join (select cast(year as int), organization,d from
    alldatasetssshare) tmp1
                                                on tmp1.organization=allpubsshare.organization and tmp1.year=allpubsshare.year
                                left outer join (select cast(year as int), organization,s from
    allsoftwaresshare) tmp2
                                                on tmp2.organization=allpubsshare.organization and cast(tmp2.year as int)=cast(allpubsshare.year as int); /*EOS*/

DROP VIEW pubs_oa; /*EOS*/
DROP VIEW datasets_oa; /*EOS*/
DROP VIEW software_oa; /*EOS*/
DROP VIEW allpubs; /*EOS*/
DROP VIEW alldatasets; /*EOS*/
DROP VIEW allsoftware; /*EOS*/
DROP VIEW allpubsshare; /*EOS*/
DROP VIEW alldatasetssshare; /*EOS*/
DROP VIEW allsoftwaresshare; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_has_preprint purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_pub_has_preprint stored as parquet as
select distinct p.id, coalesce(has_preprint, 0) as has_preprint
from ${stats_db_name}.publication_classifications p
         left outer join (
    select p.id, 1 as has_preprint
    from ${stats_db_name}.publication_classifications p
    where p.type='Preprint') tmp
                         on p.id= tmp.id; /*EOS*/
drop table if exists ${stats_db_name}.indi_pub_in_subscribed purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_pub_in_subscribed stored as parquet as
select distinct p.id, coalesce(is_subscription, 0) as is_subscription
from ${stats_db_name}.publication p
         left outer join(
    select  p.id, 1 as is_subscription from ${stats_db_name}.publication p
                                                join ${stats_db_name}.indi_pub_gold_oa g on p.id=g.id
                                                join ${stats_db_name}.indi_pub_hybrid h on p.id=h.id
                                                join ${stats_db_name}.indi_pub_in_transformative t on p.id=t.id
    where g.is_gold=0 and h.is_hybrid=0 and t.is_transformative=0) tmp
                        on p.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_with_pid purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_result_with_pid as
select distinct p.id, coalesce(result_with_pid, 0) as result_with_pid
from ${stats_db_name}.result p
         left outer join (
    select p.id, 1 as result_with_pid
    from ${stats_db_name}.result_pids p) tmp
                         on p.id= tmp.id; /*EOS*/

CREATE TEMPORARY VIEW pub_fos_totals as
select rf.id, count(distinct lvl3) totals from ${stats_db_name}.result_fos rf
group by rf.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_interdisciplinarity purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_pub_interdisciplinarity as
select distinct p.id as id, coalesce(is_interdisciplinary, 0)
as is_interdisciplinary
from pub_fos_totals p
left outer join (
select pub_fos_totals.id, 1 as is_interdisciplinary from pub_fos_totals
where totals>1) tmp on p.id=tmp.id; /*EOS*/

drop view pub_fos_totals; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_bronze_oa purge; /*EOS*/

create table ${stats_db_name}.indi_pub_bronze_oa stored as parquet as
select distinct p.id,coalesce(is_bronze_oa,0) is_bronze_oa
from ${stats_db_name}.publication p
left outer join (
    select p.id, 1 as is_bronze_oa
    from ${stats_db_name}.publication p
    join ${stats_db_name}.result_instance ri on ri.id=p.id
    join ${stats_db_name}.datasource d on d.id=ri.hostedby
    join ${stats_db_name}.indi_pub_gold_oa indi_gold on indi_gold.id=p.id
    join ${stats_db_name}.indi_pub_hybrid indi_hybrid on indi_hybrid.id=p.id
    left outer join ${stats_db_name}.result_accessroute ra on ra.id=p.id
    where indi_gold.is_gold=0 and indi_hybrid.is_hybrid=0
    and ((d.type like '%Journal%' and ri.accessright not in ('Closed Access', 'Restricted', 'Not Available') and ri.license is null) or ra.accessroute='bronze')) tmp on p.id=tmp.id; /*EOS*/

CREATE TEMPORARY VIEW project_year_result_year as
select p.id project_id, acronym, r.id result_id, r.year, p.end_year
from ${stats_db_name}.project p
join ${stats_db_name}.result_projects rp on p.id=rp.project
join ${stats_db_name}.result r on r.id=rp.id
where p.end_year is NOT NULL and r.year is not null; /*EOS*/

drop table if exists ${stats_db_name}.indi_is_project_result_after purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_is_project_result_after stored as parquet as
select pry.project_id, pry.acronym, pry.result_id,
coalesce(is_project_result_after, 0) as is_project_result_after
from project_year_result_year pry
left outer join (select pry.project_id, pry.acronym, pry.result_id, 1 as is_project_result_after
from project_year_result_year pry
where pry.year>pry.end_year) tmp on pry.result_id=tmp.result_id; /*EOS*/

drop view project_year_result_year; /*EOS*/

drop table if exists ${stats_db_name}.indi_is_funder_plan_s purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_is_funder_plan_s stored as parquet as
select distinct f.id, f.name, coalesce(is_funder_plan_s, 0) as is_funder_plan_s
from ${stats_db_name}.funder f
         left outer join (select id, name, 1 as is_funder_plan_s from ${stats_db_name}.funder
         join stats_ext.plan_s_short on c_o_alition_s_organisation_funder=name) tmp
                         on f.name= tmp.name; /*EOS*/

--Funder Fairness
drop table if exists ${stats_db_name}.indi_funder_fairness purge; /*EOS*/

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
         join result_fair on result_fair.funder=allresults.funder; /*EOS*/

--RIs Fairness
drop table if exists ${stats_db_name}.indi_ris_fairness purge; /*EOS*/

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
         join result_fair on result_fair.ri_initiative=allresults.ri_initiative; /*EOS*/

--Funder Openess

CREATE TEMPORARY VIEW pubs_oa as
select p.funder funder, count(distinct rp.id) no_oapubs from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.publication r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by p.funder; /*EOS*/


CREATE TEMPORARY VIEW datasets_oa as
select p.funder funder, count(distinct rp.id) no_oadatasets from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.dataset r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by p.funder; /*EOS*/

CREATE TEMPORARY VIEW software_oa as
select p.funder funder, count(distinct rp.id) no_oasoftware from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.software r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by p.funder; /*EOS*/

CREATE TEMPORARY VIEW allpubs as
select p.funder funder, count(distinct rp.id) no_allpubs from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.publication r on r.id=rp.id
where cast(r.year as int)>2003
group by p.funder; /*EOS*/

CREATE TEMPORARY VIEW alldatasets as
select p.funder funder, count(distinct rp.id) no_alldatasets from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.dataset r on r.id=rp.id
where cast(r.year as int)>2003
group by p.funder; /*EOS*/

CREATE TEMPORARY VIEW allsoftware as
select p.funder funder, count(distinct rp.id) no_allsoftware from ${stats_db_name}.result_projects rp
join ${stats_db_name}.project p on p.id=rp.project
join ${stats_db_name}.software r on r.id=rp.id
where cast(r.year as int)>2003
group by p.funder; /*EOS*/

CREATE TEMPORARY VIEW allpubsshare as
select pubs_oa.funder, pubs_oa.no_oapubs/allpubs.no_allpubs p from allpubs
                        join pubs_oa on allpubs.funder=pubs_oa.funder; /*EOS*/

CREATE TEMPORARY VIEW alldatasetssshare as
select datasets_oa.funder, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from alldatasets
                             join datasets_oa on alldatasets.funder=datasets_oa.funder; /*EOS*/

CREATE TEMPORARY VIEW allsoftwaresshare as
select software_oa.funder, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from allsoftware
                             join software_oa on allsoftware.funder=software_oa.funder; /*EOS*/

drop table if exists ${stats_db_name}.indi_funder_openess purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_funder_openess stored as parquet as
select allpubsshare.funder,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
           funder_openess FROM allpubsshare
                                left outer join (select funder,d from
    alldatasetssshare) tmp1
                                                on tmp1.funder=allpubsshare.funder
                                left outer join (select funder,s from
    allsoftwaresshare) tmp2
                                                on tmp2.funder=allpubsshare.funder; /*EOS*/

DROP VIEW pubs_oa; /*EOS*/
DROP VIEW datasets_oa; /*EOS*/
DROP VIEW software_oa; /*EOS*/
DROP VIEW allpubs; /*EOS*/
DROP VIEW alldatasets; /*EOS*/
DROP VIEW allsoftware; /*EOS*/
DROP VIEW allpubsshare; /*EOS*/
DROP VIEW alldatasetssshare; /*EOS*/
DROP VIEW allsoftwaresshare; /*EOS*/

--RIs Openess

CREATE TEMPORARY VIEW result_contexts as
select distinct rc.id, context.name ri_initiative from ${stats_db_name}.result_concepts rc
join ${stats_db_name}.concept on concept.id=rc.concept
join ${stats_db_name}.category on category.id=concept.category
join ${stats_db_name}.context on context.id=category.context; /*EOS*/

CREATE TEMPORARY VIEW pubs_oa as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_oapubs from result_contexts rp
join ${stats_db_name}.publication r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by rp.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW datasets_oa as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_oadatasets from result_contexts rp
join ${stats_db_name}.dataset r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by rp.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW software_oa as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_oasoftware from result_contexts rp
join ${stats_db_name}.software r on r.id=rp.id
join ${stats_db_name}.result_instance ri on ri.id=r.id
where (ri.accessright = 'Open Access' or ri.accessright = 'Embargo'  or ri.accessright = 'Open Source')
and cast(r.year as int)>2003
group by rp.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW allpubs as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_allpubs from result_contexts rp
join ${stats_db_name}.publication r on r.id=rp.id
where cast(r.year as int)>2003
group by rp.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW alldatasets as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_alldatasets from result_contexts rp
join ${stats_db_name}.dataset r on r.id=rp.id
where cast(r.year as int)>2003
group by rp.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW allsoftware as
select rp.ri_initiative ri_initiative, count(distinct rp.id) no_allsoftware from result_contexts rp
join ${stats_db_name}.software r on r.id=rp.id
where cast(r.year as int)>2003
group by rp.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW allpubsshare as
select pubs_oa.ri_initiative, pubs_oa.no_oapubs/allpubs.no_allpubs p from allpubs
                        join pubs_oa on allpubs.ri_initiative=pubs_oa.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW alldatasetssshare as
select datasets_oa.ri_initiative, datasets_oa.no_oadatasets/alldatasets.no_alldatasets d
                             from alldatasets
                             join datasets_oa on alldatasets.ri_initiative=datasets_oa.ri_initiative; /*EOS*/

CREATE TEMPORARY VIEW allsoftwaresshare as
select software_oa.ri_initiative, software_oa.no_oasoftware/allsoftware.no_allsoftware s
                             from allsoftware
                             join software_oa on allsoftware.ri_initiative=software_oa.ri_initiative; /*EOS*/

drop table if exists ${stats_db_name}.indi_ris_openess purge; /*EOS*/

create table if not exists ${stats_db_name}.indi_ris_openess stored as parquet as
select allpubsshare.ri_initiative,
       (p+if(isnull(s),0,s)+if(isnull(d),0,d))/(1+(case when s is null then 0 else 1 end)
           +(case when d is null then 0 else 1 end))
	ris_openess FROM allpubsshare
                                left outer join (select ri_initiative,d from
    alldatasetssshare) tmp1
                                                on tmp1.ri_initiative=allpubsshare.ri_initiative
                                left outer join (select ri_initiative,s from
    allsoftwaresshare) tmp2
                                                on tmp2.ri_initiative=allpubsshare.ri_initiative; /*EOS*/

DROP VIEW result_contexts; /*EOS*/
DROP VIEW pubs_oa; /*EOS*/
DROP VIEW datasets_oa; /*EOS*/
DROP VIEW software_oa; /*EOS*/
DROP VIEW allpubs; /*EOS*/
DROP VIEW alldatasets; /*EOS*/
DROP VIEW allsoftware; /*EOS*/
DROP VIEW allpubsshare; /*EOS*/
DROP VIEW alldatasetssshare; /*EOS*/
DROP VIEW allsoftwaresshare; /*EOS*/

--Funder Findability
drop table if exists ${stats_db_name}.indi_funder_findable purge; /*EOS*/

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
         join result_findable on result_findable.funder=allresults.funder; /*EOS*/

--RIs Findability
drop table if exists ${stats_db_name}.indi_ris_findable purge; /*EOS*/

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
         join result_findable on result_findable.ri_initiative=allresults.ri_initiative; /*EOS*/

create table if not exists ${stats_db_name}.indi_pub_publicly_funded stored as parquet as
with org_names_pids as
(select org.id,name, pid from ${stats_db_name}.organization org
join ${stats_db_name}.organization_pids op on org.id=op.id),
publicly_funded_orgs as
(select distinct name from
(select pf.name from stats_ext.insitutions_for_publicly_funded pf
join ${stats_db_name}.fundref f on f.name=pf.name where f.type='government'
union all
select pf.name from stats_ext.insitutions_for_publicly_funded pf
join ${stats_db_name}.project p on p.funder=pf.name
union all
select op.name from stats_ext.insitutions_for_publicly_funded pf
join org_names_pids op on (op.name=pf.name or op.pid=pf.ror)
and pf.publicly_funded='yes') foo)
select distinct p.id, coalesce(publicly_funded, 0) as publicly_funded
from ${stats_db_name}.publication p
left outer join (
select distinct ro.id, 1 as publicly_funded from ${stats_db_name}.result_organization ro
join ${stats_db_name}.organization o on o.id=ro.organization
join publicly_funded_orgs pfo on o.name=pfo.name) tmp on p.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_pub_green_with_license purge; /*EOS*/
create table ${stats_db_name}.indi_pub_green_with_license stored as parquet as
select distinct p.id, coalesce(green_with_license, 0) as green_with_license
from ${stats_db_name}.publication p
left outer join (
    select distinct p.id, 1 as green_with_license from ${stats_db_name}.publication p
    join ${stats_db_name}.result_instance ri on ri.id = p.id
    join ${stats_db_name}.datasource on datasource.id = ri.hostedby
    where ri.license is not null and datasource.type like '%Repository%' and datasource.name!='Other') tmp on p.id= tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.result_country purge; /*EOS*/

create table ${stats_db_name}.result_country stored as parquet as
select distinct *
from (
    select ro.id, o.country
    from ${stats_db_name}.result_organization ro
    left outer join ${stats_db_name}.organization o on o.id=ro.organization
    union all
    select rp.id, f.country
    from ${stats_db_name}.result_projects
    left outer join ${stats_db_name}.project p on p.id=rp.project
    left outer join ${stats_db_name}.funder f on f.name=p.funder
     ) rc
where rc.country is not null; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_oa_with_license purge; /*EOS*/
create table ${stats_db_name}.indi_result_oa_with_license stored as parquet as
select distinct r.id, coalesce(oa_with_license,0) as oa_with_license
from ${stats_db_name}.result r
left outer join (select distinct r.id, 1 as oa_with_license from ${stats_db_name}.result r
join ${stats_db_name}.result_licenses rl on rl.id=r.id where r.bestlicence='Open Access') tmp on r.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_oa_without_license purge; /*EOS*/
create table ${stats_db_name}.indi_result_oa_without_license stored as parquet as
with without_license as
(select distinct id from ${stats_db_name}.indi_result_oa_with_license
where oa_with_license=0)
select distinct r.id, coalesce(oa_without_license,0) as oa_without_license
from ${stats_db_name}.result r
left outer join (select distinct r.id, 1 as oa_without_license
from ${stats_db_name}.result r
join without_license wl on wl.id=r.id
where r.bestlicence='Open Access') tmp on r.id=tmp.id; /*EOS*/

drop table if exists ${stats_db_name}.indi_result_under_transformative purge; /*EOS*/
create table ${stats_db_name}.indi_result_under_transformative stored as parquet as
with transformative_dois as (
    select distinct doi from stats_ext.transformative_facts)
select distinct r.id, coalesce(under_transformative,0) as under_transformative
from ${stats_db_name}.result r
left outer join (
    select distinct rp.id, 1 as under_transformative
    from ${stats_db_name}.result_pids rp join ${stats_db_name}.result r on r.id=rp.id
    join transformative_dois td on td.doi=rp.pid) tmp on r.id=tmp.id; /*EOS*/