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
             'openorgs____::b84450f9864182c67b8611b5593f4250', --"Athena Research and Innovation Center In Information Communication & Knowledge Technologies', --ARC"
             'openorgs____::d41cf6bd4ab1b1362a44397e0b95c975', --National Research Council
             'openorgs____::d2a09b9d5eabb10c95f9470e172d05d2', --??? Not exists ??
             'openorgs____::d169c7407dd417152596908d48c11460', --Masaryk University
             'openorgs____::1ec924b1759bb16d0a02f2dad8689b21', --University of Belgrade
             'openorgs____::2fb1e47b4612688d9de9169d579939a7', --University of Helsinki
             'openorgs____::759d59f05d77188faee99b7493b46805', --University of Minho
             'openorgs____::cad284878801b9465fa51a95b1d779db', --Universidad Politécnica de Madrid
             'openorgs____::eadc8da90a546e98c03f896661a2e4d4', --University of Göttingen
             'openorgs____::c0286313e36479eff8676dba9b724b40', --National and Kapodistrian University of Athens
             -- 'openorgs____::c80a8243a5e5c620d7931c88d93bf17a', --Université Paris Diderot
             'openorgs____::c08634f0a6b0081c3dc6e6c93a4314f3', --Bielefeld University
             'openorgs____::6fc85e4a8f7ecaf4b0c738d010e967ea', --University of Southern Denmark
             'openorgs____::3d6122f87f9a97a99d8f6e3d73313720', --Humboldt-Universität zu Berlin
             'openorgs____::16720ada63d0fa8ca41601feae7d1aa5', --TU Darmstadt
             'openorgs____::ccc0a066b56d2cfaf90c2ae369df16f5', --KU Leuven
             'openorgs____::4c6f119632adf789746f0a057ed73e90', --University of the Western Cape
             'openorgs____::ec3665affa01aeafa28b7852c4176dbd', --Rudjer Boskovic Institute
             'openorgs____::5f31346d444a7f06a28c880fb170b0f6', --Ghent University
             'openorgs____::2dbe47117fd5409f9c61620813456632', --University of Luxembourg
             'openorgs____::6445d7758d3a40c4d997953b6632a368' --National Institute of Informatics (NII)
        ) )) foo;
compute stats TARGET.result;

create table TARGET.result_citations stored as parquet as select * from SOURCE.result_citations orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_citations;

create table TARGET.result_references_oc stored as parquet as select * from SOURCE.result_references_oc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_references_oc;

create table TARGET.result_citations_oc stored as parquet as select * from SOURCE.result_citations_oc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_citations_oc;

create table TARGET.result_classifications stored as parquet as select * from SOURCE.result_classifications orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_classifications;

create table TARGET.result_apc stored as parquet as select * from SOURCE.result_apc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_apc;

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

create table TARGET.result_licenses stored as parquet as select * from SOURCE.result_licenses orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.result_licenses;

create table TARGET.licenses_normalized STORED AS PARQUET as select * from SOURCE.licenses_normalized;

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

create view TARGET.foo1 as select * from SOURCE.result_result rr where rr.source in (select id from TARGET.result);
create view TARGET.foo2 as select * from SOURCE.result_result rr where rr.target in (select id from TARGET.result);
create table TARGET.result_result STORED AS PARQUET as select distinct * from (select * from TARGET.foo1 union all select * from TARGET.foo2) foufou;
drop view TARGET.foo1;
drop view TARGET.foo2;
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
create view if not exists TARGET.project_classification as select * from SOURCE.project_classification;

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
create table TARGET.indi_datasets_gold_oa stored as parquet as select * from SOURCE.indi_datasets_gold_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_datasets_gold_oa;
create table TARGET.indi_software_gold_oa stored as parquet as select * from SOURCE.indi_software_gold_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_software_gold_oa;
create table TARGET.indi_pub_has_abstract stored as parquet as select * from SOURCE.indi_pub_has_abstract orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_has_abstract;
create table TARGET.indi_result_has_cc_licence stored as parquet as select * from SOURCE.indi_result_has_cc_licence orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_result_has_cc_licence;
create table TARGET.indi_result_has_cc_licence_url stored as parquet as select * from SOURCE.indi_result_has_cc_licence_url orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_result_has_cc_licence_url;

create view TARGET.indi_funder_country_collab as select * from SOURCE.indi_funder_country_collab;

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

create table TARGET.indi_result_no_of_copies stored as parquet as select * from SOURCE.indi_result_no_of_copies orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_result_no_of_copies;

create view TARGET.indi_org_findable as select * from SOURCE.indi_org_findable;
create view TARGET.indi_org_openess as select * from SOURCE.indi_org_openess;
create table TARGET.indi_pub_hybrid_oa_with_cc stored as parquet as select * from SOURCE.indi_pub_hybrid_oa_with_cc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
compute stats TARGET.indi_pub_hybrid_oa_with_cc;

create table TARGET.indi_pub_downloads stored as parquet as select * from SOURCE.indi_pub_downloads orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
compute stats TARGET.indi_pub_downloads;
create table TARGET.indi_pub_downloads_datasource stored as parquet as select * from SOURCE.indi_pub_downloads_datasource orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
compute stats TARGET.indi_pub_downloads_datasource;
create table TARGET.indi_pub_downloads_year stored as parquet as select * from SOURCE.indi_pub_downloads_year orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
compute stats TARGET.indi_pub_downloads_year;
create table TARGET.indi_pub_downloads_datasource_year stored as parquet as select * from SOURCE.indi_pub_downloads_datasource_year orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
compute stats TARGET.indi_pub_downloads_datasource_year;

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