create table indi_pub_green_oa stored as parquet as
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

create table indi_pub_grey_lit stored as parquet as
select distinct p.id, coalesce(grey_lit, 0) as grey_lit
from publication p
left outer join (
select p.id, 1 as grey_lit 
from publication p
join result_classifications rt on rt.id = p.id
where rt.type not in ('Article','Part of book or chapter of book','Book','Doctoral thesis','Master thesis','Data Paper', 'Thesis', 'Bachelor thesis', 'Conference object') and 
not exists (select 1 from result_classifications rc where type ='Other literature type' and rc.id=p.id)) tmp on p.id=tmp.id;

create table indi_pub_doi_from_crossref stored as parquet as
select distinct p.id, coalesce(doi_from_crossref, 0) as doi_from_crossref 
from publication p
left outer join
(select ri.id, 1 as doi_from_crossref from result_instance ri
join datasource d on d.id = ri.collectedfrom
where pidtype='Digital Object Identifier' and d.name ='Crossref') tmp
on tmp.id=p.id;

create table indi_pub_gold_oa stored as parquet as
select distinct p.id, coalesce(gold_oa, 0) as gold_oa
from publication p
left outer join ( 
select p.id, 1 as gold_oa
from publication p
join result_instance ri on ri.id = p.id
join datasource on datasource.id = ri.hostedby
where datasource.id like '%doajarticles%') tmp
on p.id= tmp.id;

--create table indi_project_pubs_count stored as parquet as
--select  pr.id id, count(p.id) total_pubs from project_results pr
--join publication p on p.id=pr.result
--group by pr.id;

--create table indi_project_datasets_count stored as parquet as
--select pr.id id, count(d.id) total_datasets from project_results pr
--join dataset d on d.id=pr.result
--group by pr.id;

--create table indi_project_software_count stored as parquet as
--select  pr.id id, count(s.id) total_software from project_results pr
--join software s on s.id=pr.result
--group by pr.id;

--create table indi_project_otherresearch_count stored as parquet as
--select pr.id id, count(o.id) total_other from project_results pr
--join otherresearchproduct o on o.id=pr.result
--group by pr.id;

--create table indi_pub_avg_year_country_oa stored as parquet as
--select year, country, round(OpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageOA,
--round(NonOpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageNonOA
 --from
 --(SELECT year, country, SUM(CASE
    --WHEN bestlicence='Open Access' THEN 1
 --ELSE 0
 --END) AS OpenAccess, SUM(CASE
 --WHEN bestlicence<>'Open Access' THEN 1
 --ELSE 0
 --END) AS NonOpenAccess
 --FROM publication p
 --join result_organization ro on p.id=ro.id
 --join organization o on o.id=ro.organization
 --where cast(year as int)>=2003 and cast(year as int)<=2021
 --group by year, country) tmp;

--create table indi_dataset_avg_year_country_oa stored as parquet as
--select year, country, round(OpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageOA,
--round(NonOpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageNonOA
--from
 --(SELECT year, country, SUM(CASE
    --WHEN bestlicence='Open Access' THEN 1
 --ELSE 0
 --END) AS OpenAccess, SUM(CASE
 --WHEN bestlicence<>'Open Access' THEN 1
 --ELSE 0
 --END) AS NonOpenAccess
 --FROM dataset d
 --join result_organization ro on d.id=ro.id
 --join organization o on o.id=ro.organization
 --where cast(year as int)>=2003 and cast(year as int)<=2021
 --group by year, country) tmp;

--create table indi_software_avg_year_country_oa stored as parquet as
--select year, country, round(OpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageOA,
--round(NonOpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageNonOA
--from
-- (SELECT year, country, SUM(CASE
    --WHEN bestlicence='Open Access' THEN 1
-- ELSE 0
--END) AS OpenAccess, SUM(CASE
-- WHEN bestlicence<>'Open Access' THEN 1
-- ELSE 0
-- END) AS NonOpenAccess
-- FROM software s
-- join result_organization ro on s.id=ro.id
-- join organization o on o.id=ro.organization
-- where cast(year as int)>=2003 and cast(year as int)<=2021
-- group by year, country) tmp;


--create table indi_other_avg_year_country_oa stored as parquet as
--select year, country, round(OpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageOA,
--round(NonOpenAccess/(OpenAccess+NonOpenAccess)*100,3) as averageNonOA
-- from
-- (SELECT year, country, SUM(CASE
--    WHEN bestlicence='Open Access' THEN 1
-- ELSE 0
-- END) AS OpenAccess, SUM(CASE
-- WHEN bestlicence<>'Open Access' THEN 1
-- ELSE 0
-- END) AS NonOpenAccess
-- FROM otherresearchproduct orp
-- join result_organization ro on orp.id=ro.id
-- join organization o on o.id=ro.organization
-- where cast(year as int)>=2003 and cast(year as int)<=2021
-- group by year, country) tmp;

--create table indi_pub_avg_year_context_oa stored as parquet as
--with total as
--(select count(distinct pc.id) no_of_pubs, year, c.name name, sum(count(distinct pc.id)) over(PARTITION by year) as total from publication_concepts pc
--join context c on pc.concept like concat('%',c.id,'%')
--join publication p on p.id=pc.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by c.name, year )
--select year, name, round(no_of_pubs/total*100,3) averageofpubs
--from total;

--create table indi_dataset_avg_year_context_oa stored as parquet as
--with total as
--(select count(distinct pc.id) no_of_pubs, year, c.name name, sum(count(distinct pc.id)) over(PARTITION by year) as total from dataset_concepts pc
--join context c on pc.concept like concat('%',c.id,'%')
--join dataset p on p.id=pc.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by c.name, year )
--select year, name, round(no_of_pubs/total*100,3) averageofdataset
--from total;

--create table indi_software_avg_year_context_oa stored as parquet as
--with total as
--(select count(distinct pc.id) no_of_pubs, year, c.name name, sum(count(distinct pc.id)) over(PARTITION by year) as total from software_concepts pc
--join context c on pc.concept like concat('%',c.id,'%')
--join software p on p.id=pc.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by c.name, year )
--select year, name, round(no_of_pubs/total*100,3) averageofsoftware
--from total;

--create table indi_other_avg_year_context_oa stored as parquet as
--with total as
--(select count(distinct pc.id) no_of_pubs, year, c.name name, sum(count(distinct pc.id)) over(PARTITION by year) as total from otherresearchproduct_concepts pc
--join context c on pc.concept like concat('%',c.id,'%')
--join otherresearchproduct p on p.id=pc.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by c.name, year )
--select year, name, round(no_of_pubs/total*100,3) averageofother
--from total;

--create table indi_other_avg_year_content_oa stored as parquet as
--with total as
--(select count(distinct pd.id) no_of_pubs, year, d.type type, sum(count(distinct pd.id)) over(PARTITION by year) as total
--from otherresearchproduct_datasources pd
--join datasource d on datasource=d.id
--join otherresearchproduct p on p.id=pd.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by d.type, year)
--select year, type, round(no_of_pubs/total*100,3) averageOfOtherresearchproduct
--from total;

--create table indi_software_avg_year_content_oa stored as parquet as
--with total as
--(select count(distinct pd.id) no_of_pubs, year, d.type type, sum(count(distinct pd.id)) over(PARTITION by year) as total
--from software_datasources pd
--join datasource d on datasource=d.id
--join software p on p.id=pd.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by d.type, year)
--select year, type, round(no_of_pubs/total*100,3) averageOfSoftware
--from total;

--create table indi_dataset_avg_year_content_oa stored as parquet as
--with total as
--(select count(distinct pd.id) no_of_pubs, year, d.type type, sum(count(distinct pd.id)) over(PARTITION by year) as total
--from dataset_datasources pd
--join datasource d on datasource=d.id
--join dataset p on p.id=pd.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by d.type, year)
--select year, type, round(no_of_pubs/total*100,3) averageOfDatasets
--from total;

--create table indi_pub_avg_year_content_oa stored as parquet as
--with total as
--(select count(distinct pd.id) no_of_pubs, year, d.type type, sum(count(distinct pd.id)) over(PARTITION by year) as total
--from publication_datasources pd
--join datasource d on datasource=d.id
--join publication p on p.id=pd.id
--where cast(year as int)>=2003 and cast(year as int)<=2021
--group by d.type, year)
--select year, type, round(no_of_pubs/total*100,3) averageOfPubs
--from total;

create table indi_pub_has_cc_licence stored as parquet as
select distinct p.id, (case when lic='' or lic is null then 0 else 1 end) as has_cc_license
from publication p
left outer join (select p.id, license.type as lic from publication p
join publication_licenses as license on license.id = p.id
where lower(license.type) LIKE '%creativecommons.org%' OR lower(license.type) LIKE '%cc-%') tmp
on p.id= tmp.id;

create table indi_pub_has_cc_licence_url stored as parquet as
select distinct p.id, (case when lic_host='' or lic_host is null then 0 else 1 end) as has_cc_license_url
from publication p
left outer join (select p.id, lower(parse_url(license.type, "HOST")) as lic_host
from publication p
join publication_licenses as license on license.id = p.id
WHERE lower(parse_url(license.type, 'HOST')) = 'creativecommons.org') tmp
on p.id= tmp.id;

-- EOSC-TR1.1-02M:
-- ## Indicator: has_cc_license. Creative Commons licensing has become a
-- de facto standard in scholarly communication and is promoted by many initiatives
-- like Plan S. This indicator might be only useful when applied 
-- to openly available publications.
--create table indi_pub_has_cc_licence_tr stored as parquet as  
--select distinct p.id, case when lic='' or lic is null then 0 else 1 end as has_cc_license_tr
--from publication p  
--left outer join (select p.id, license.type as lic from publication p
--join publication_licenses as license on license.id = p.id  
--where lower(license.type) LIKE '%creativecommons.org%' OR lower(license.type) LIKE '%cc-%') tmp  
--on p.id= tmp.id

-- #EOSC-F2-01M_cc  Rich metadata for scholarly publications
-- ## Indicator: has_cc_license. Creative Commons licensing has become a
-- de facto standard in scholarly communication and is promoted by many initiatives
-- like Plan S. This indicator might be only useful when applied 
-- to openly available publications.

-- Same indicator as EOSC-TR1.1-02M (Najko's instructions)
-- create table indi_pub_has_cc_licence_f stored as parquet as
-- select
-- distinct p.id, case when lic='' or lic is null then 0 else 1 end as has_cc_license_f
-- from publication p  
-- left outer join (selectp.id,license.type as lic from publication p  
-- join publication_licenses as license on license.id = p.id  
-- where lower(license.type) LIKE '%creativecommons.org%' OR lower(license.type) LIKE '%cc-%') tmp  
-- on p.id= tmp.id

create table indi_pub_has_abstract stored as parquet as
select distinct publication.id, coalesce(abstract, 1) has_abstract
from publication;

create table indi_result_with_orcid stored as parquet as  
select distinct r.id, coalesce(has_orcid, 0) as has_orcid
from result r 
left outer join (select id, 1 as has_orcid from result_orcid) tmp 
on r.id= tmp.id 

create table indi_funded_result_with_fundref stored as parquet as  
select distinct r.id, coalesce(fundref, 0) as fundref
from project_results r 
left outer join (select distinct id, 1 as fundref from project_results
where provenance='Harvested') tmp 
on r.id= tmp.id

create table indi_result_org_country_collab stored as parquet as  
with tmp as 
(select o.id as id, o.country , ro.id as result,r.type  from organization o
join result_organization ro on o.id=ro.organization
join result r on r.id=ro.id where o.country <> 'UNKNOWN')
select o1.id org1,o2.country country2, o1.type, count(distinct o1.result) as collaborations
from tmp as o1
join tmp as o2 on o1.result=o2.result
where o1.id<>o2.id and o1.country<>o2.country 
group by o1.id, o1.type,o2.country

create table indi_result_org_collab stored as parquet as  
with tmp as 
(select o.id, ro.id as result,r.type  from organization o
join result_organization ro on o.id=ro.organization
join result r on r.id=ro.id)
select o1.id org1,o2.id org2, o1.type, count(distinct o1.result) as collaborations
from tmp as o1
join tmp as o2 on o1.result=o2.result
where o1.id<>o2.id
group by o1.id, o2.id, o1.type

create table indi_funder_country_collab stored as parquet as  
with tmp as (select funder, project, country from organization_projects op 
join organization o on o.id=op.id 
join project p on p.id=op.project 
where country <> 'UNKNOWN')
select f1.funder, f1.country, f2.country, count(distinct f1.project) as collaborations
from tmp as f1
join tmp as f2 on f1.project=f2.project
where f1.country<>f2.country 
group by f1.funder, f2.country, f1.country

create table indi_pub_diamond stored as parquet as
select distinct pd.id, coalesce(in_diamond_journal, 0) as in_diamond_journal 
from publication_datasources pd
left outer join (
select pd.id, 1 as in_diamond_journal from publication_datasources pd
join datasource d on d.id=pd.datasource
join stats_ext.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
and (ps.journal_is_in_doaj=true or ps.journal_is_oa=true) and ps.has_apc=false) tmp
on pd.id=tmp.id

create table indi_pub_hybrid stored as parquet as
select distinct pd.id, coalesce(is_hybrid, 0) as is_hybrid 
from publication_datasources pd
left outer join (
select pd.id, 1 as is_hybrid from publication_datasources pd
join datasource d on d.id=pd.datasource
join stats_ext.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
and (ps.journal_is_in_doaj=false and ps.journal_is_oa=false)) tmp
on pd.id=tmp.id

create table indi_is_gold_oa stored as parquet as
(select distinct pd.id, coalesce(gold_oa, 0) as gold_oa 
from publication_datasources pd
left outer join (
select pd.id, 1 as gold_oa from publication_datasources pd
join datasource d on d.id=pd.datasource
join stats_ext.plan_s_jn ps on (ps.issn_print=d.issn_printed or ps.issn_online=d.issn_online)
where ps.journal_is_in_doaj is true or ps.journal_is_oa is true) tmp
on pd.id=tmp.id)


create table indi_pub_in_transformative stored as parquet as
select distinct pd.id, coalesce(is_transformative, 0) as is_transformative 
from publication pd
left outer join (
select  pd.id, 1 as is_transformative from publication_datasources pd
join datasource d on d.id=pd.datasource
join stats_ext.plan_s_jn ps where (ps.issn_print=d.issn_printed and ps.issn_online=d.issn_online)
and ps.is_transformative_journal=true) tmp
on pd.id=tmp.id

create table indi_pub_closed_other_open stored as parquet as
select distinct ri.id, coalesce(pub_closed_other_open, 0) as pub_closed_other_open from result_instance ri
left outer join 
(select ri.id, 1 as pub_closed_other_open from result_instance ri
join publication p on p.id=ri.id
join datasource d on ri.hostedby=d.id
where d.type like '%Journal%' and ri.accessright='Closed Access' and
(p.bestlicence='Open Access' or p.bestlicence='Open Source')) tmp 
on tmp.id=ri.id