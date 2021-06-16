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
--create view if not exists TARGET.roarmap as select * from SOURCE.roarmap;

create table TARGET.result as
    select distinct * from (
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_projects rp join SOURCE.project p on rp.project=p.id where rp.id=r.id)
        union all
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_concepts rc where rc.id=r.id)
        union all
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_project rp join SOURCE.project p on p.id=rp.project join SOURCE.project_organizations po on po.id=p.id join SOURCE.organization o on o.id=po.organization where ro.id=r.id and o.name in (
            'GEORG-AUGUST-UNIVERSITAT GOTTINGEN STIFTUNG OFFENTLICHEN RECHTS',
            'ATHINA-EREVNITIKO KENTRO KAINOTOMIAS STIS TECHNOLOGIES TIS PLIROFORIAS, TON EPIKOINONION KAI TIS GNOSIS',
            'Consiglio Nazionale delle Ricerche',
            'Universidade do Minho') )) foo;
compute stats TARGET.result;

create table TARGET.result_citations as select * from SOURCE.result_citations orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_citations;

create table TARGET.result_classifications as select * from SOURCE.result_classifications orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_classifications;

create table TARGET.result_concepts as select * from SOURCE.result_concepts orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_concepts;

create table TARGET.result_datasources as select * from SOURCE.result_datasources orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_datasources;

create table TARGET.result_fundercount as select * from SOURCE.result_fundercount orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_fundercount;

create table TARGET.result_gold as select * from SOURCE.result_gold orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_gold;

create table TARGET.result_greenoa as select * from SOURCE.result_greenoa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_greenoa;

create table TARGET.result_languages as select * from SOURCE.result_languages orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_languages;

create table TARGET.result_licences as select * from SOURCE.result_licenses orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_licences;

create table TARGET.result_oids as select * from SOURCE.result_oids orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_oids;

create table TARGET.result_organization as select * from SOURCE.result_organization orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_organization;

create table TARGET.result_peerreviewed as select * from SOURCE.result_peerreviewed orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_peerreviewed;

create table TARGET.result_pids as select * from SOURCE.result_pids orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_pids;

create table TARGET.result_projectcount as select * from SOURCE.result_projectcount orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_projectcount;

create table TARGET.result_projects as select * from SOURCE.result_projects orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_projects;

create table TARGET.result_refereed as select * from SOURCE.result_refereed orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_refereed;

create table TARGET.result_sources as select * from SOURCE.result_sources orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_sources;

create table TARGET.result_topics as select * from SOURCE.result_topics orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_topics;

-- datasources
create view if not exists TARGET.datasource as select * from SOURCE.datasource;
create view if not exists TARGET.datasource_oids as select * from SOURCE.datasource_oids;
create view if not exists TARGET.datasource_organizations as select * from SOURCE.datasource_organizations;
create view if not exists TARGET.datasource_sources as select * from SOURCE.datasource_sources;

create table TARGET.datasource_results as select id as result, datasource as id from TARGET.result_datasources;
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

create table TARGET.project_results as select id as result, project as id from TARGET.result_projects;
compute stats TARGET.project_results;

--denorm
alter table TARGET.result rename to TARGET.res_tmp;

create table TARGET.result_denorm as
    select distinct r.*, rp.project, p.acronym as pacronym, p.title as ptitle, p.funder as pfunder, p.funding_lvl0 as pfunding_lvl0, rd.datasource, d.name as dname, d.type as dtype
    from TARGET.res_tmp r
    join TARGET.result_projects rp on rp.id=r.id
    join TARGET.result_datasources rd on rd.id=r.id
    join TARGET.project p on p.id=rp.project
    join TARGET.datasource d on d.id=rd.datasource;
compute stats TARGET.result_denorm;

alter table TARGET.result_denorm rename to TARGET.result;
drop table TARGET.res_tmp;
--- done!