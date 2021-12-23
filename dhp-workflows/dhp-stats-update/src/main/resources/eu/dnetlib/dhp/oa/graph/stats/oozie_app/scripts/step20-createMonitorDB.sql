drop database if exists TARGET cascade;
create database if not exists TARGET;

create view if not exists TARGET.category as select * from SOURCE.category;
create view if not exists TARGET.concept as select * from SOURCE.concept;
create view if not exists TARGET.context as select * from SOURCE.context;
create view if not exists TARGET.country as select * from SOURCE.country;
create view if not exists TARGET.countrygdp as select * from SOURCE.countrygdp;
create view if not exists TARGET.creation_date as select * from SOURCE.creation_date;
create view if not exists TARGET.funder as select * from SOURCE.funder;
create view if not exists TARGET.fundref as select * from SOURCE.fundref;
create view if not exists TARGET.rndexpenditure as select * from SOURCE.rndexpediture;

create table TARGET.result stored as parquet as
    select distinct * from (
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_projects rp join SOURCE.project p on rp.project=p.id where rp.id=r.id)
        union all
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_concepts rc where rc.id=r.id)
        union all
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_organization ro where ro.id=r.id and ro.organization in (
            'openorgs____::759d59f05d77188faee99b7493b46805',
            'openorgs____::b84450f9864182c67b8611b5593f4250',
            'openorgs____::d41cf6bd4ab1b1362a44397e0b95c975',
            'openorgs____::eadc8da90a546e98c03f896661a2e4d4',
            'openorgs____::d2a09b9d5eabb10c95f9470e172d05d2') )) foo;
compute stats TARGET.result;

create table TARGET.result_citations stored as parquet as select * from SOURCE.result_citations orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_citations;

create table TARGET.result_classifications stored as parquet as select * from SOURCE.result_classifications orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_classifications;

create table TARGET.result_concepts stored as parquet as select * from SOURCE.result_concepts orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_concepts;

create table TARGET.result_datasources stored as parquet as select * from SOURCE.result_datasources orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_datasources;

create table TARGET.result_fundercount stored as parquet as select * from SOURCE.result_fundercount orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_fundercount;

create table TARGET.result_gold stored as parquet as select * from SOURCE.result_gold orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_gold;

create table TARGET.result_greenoa stored as parquet as select * from SOURCE.result_greenoa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_greenoa;

create table TARGET.result_languages stored as parquet as select * from SOURCE.result_languages orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_languages;

create table TARGET.result_licences stored as parquet as select * from SOURCE.result_licenses orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_licences;

create table TARGET.result_oids stored as parquet as select * from SOURCE.result_oids orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_oids;

create table TARGET.result_organization stored as parquet as select * from SOURCE.result_organization orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_organization;

create table TARGET.result_peerreviewed stored as parquet as select * from SOURCE.result_peerreviewed orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_peerreviewed;

create table TARGET.result_pids stored as parquet as select * from SOURCE.result_pids orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_pids;

create table TARGET.result_projectcount stored as parquet as select * from SOURCE.result_projectcount orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_projectcount;

create table TARGET.result_projects stored as parquet as select * from SOURCE.result_projects orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_projects;

create table TARGET.result_refereed stored as parquet as select * from SOURCE.result_refereed orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_refereed;

create table TARGET.result_sources stored as parquet as select * from SOURCE.result_sources orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_sources;

create table TARGET.result_topics stored as parquet as select * from SOURCE.result_topics orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_topics;

create table TARGET.result_result stored as parquet as select * from SOURCE.result_result orig where exists (select 1 from TARGET.result r where r.id=orig.source or r.id=orig.target);
compute stats TARGET.result_result;

-- datasources
create view if not exists TARGET.datasource as select * from SOURCE.datasource;
create view if not exists TARGET.datasource_oids as select * from SOURCE.datasource_oids;
create view if not exists TARGET.datasource_organizations as select * from SOURCE.datasource_organizations;
create view if not exists TARGET.datasource_sources as select * from SOURCE.datasource_sources;

create table TARGET.datasource_results stored as parquet as select id as result, datasource as id from TARGET.result_datasources;
compute stats TARGET.datasource_results;

-- organizations
create view if not exists TARGET.organization as select * from SOURCE.organization;
create view if not exists TARGET.organization_datasources as select * from SOURCE.organization_datasources;
create view if not exists TARGET.organization_pids as select * from SOURCE.organization_pids;
create view if not exists TARGET.organization_projects as select * from SOURCE.organization_projects;
create view if not exists TARGET.organization_sources as select * from SOURCE.organization_sources;

-- projects
create view if not exists TARGET.project as select * from SOURCE.project;
create view if not exists TARGET.project_oids as select * from SOURCE.project_oids;
create view if not exists TARGET.project_organizations as select * from SOURCE.project_organizations;
create view if not exists TARGET.project_resultcount as select * from SOURCE.project_resultcount;

create table TARGET.project_results stored as parquet as select id as result, project as id from TARGET.result_projects;
compute stats TARGET.project_results;

-- indicators
create table TARGET.indi_pub_green_oa stored as parquet as select * from SOURCE.indi_pub_green_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_green_oa;
create table TARGET.indi_pub_grey_lit stored as parquet as select * from SOURCE.indi_pub_grey_lit orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_grey_lit;
create table TARGET.indi_pub_doi_from_crossref stored as parquet as select * from SOURCE.indi_pub_doi_from_crossref orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_doi_from_crossref;
create table TARGET.indi_pub_gold_oa stored as parquet as select * from SOURCE.indi_pub_gold_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_gold_oa;
create table TARGET.indi_pub_has_abstract stored as parquet as select * from SOURCE.indi_pub_has_abstract orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_has_abstract;
create table TARGET.indi_pub_has_cc_licence stored as parquet as select * from SOURCE.indi_pub_has_cc_licence orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_has_cc_licence;
create table TARGET.indi_pub_has_cc_licence_url stored as parquet as select * from SOURCE.indi_pub_has_cc_licence_url orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_has_cc_licence_url;

create view TARGET.indi_funder_country_collab stored as select * from SOURCE.indi_funder_country_collab;

create table TARGET.indi_result_with_orcid stored as parquet as select * from SOURCE.indi_result_with_orcid orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_result_with_orcid;
create table TARGET.indi_funded_result_with_fundref stored as parquet as select * from SOURCE.indi_funded_result_with_fundref orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_funded_result_with_fundref;
create table TARGET.indi_pub_diamond stored as parquet as select * from SOURCE.indi_pub_diamond orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_diamond;
create table TARGET.indi_pub_hybrid stored as parquet as select * from SOURCE.indi_pub_hybrid orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_hybrid;
create table TARGET.indi_pub_in_transformative stored as parquet as select * from SOURCE.indi_pub_in_transformative orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_in_transformative;
create table TARGET.indi_pub_closed_other_open stored as parquet as select * from SOURCE.indi_pub_closed_other_open orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_closed_other_open;

--denorm
alter table TARGET.result rename to TARGET.res_tmp;

create table TARGET.result_denorm stored as parquet as
    select distinct r.*, rp.project, p.acronym as pacronym, p.title as ptitle, p.funder as pfunder, p.funding_lvl0 as pfunding_lvl0, rd.datasource, d.name as dname, d.type as dtype
    from TARGET.res_tmp r
    left outer join TARGET.result_projects rp on rp.id=r.id
    left outer join TARGET.result_datasources rd on rd.id=r.id
    left outer join TARGET.project p on p.id=rp.project
    left outer join TARGET.datasource d on d.id=rd.datasource;
compute stats TARGET.result_denorm;

alter table TARGET.result_denorm rename to TARGET.result;
drop table TARGET.res_tmp;
--- done!