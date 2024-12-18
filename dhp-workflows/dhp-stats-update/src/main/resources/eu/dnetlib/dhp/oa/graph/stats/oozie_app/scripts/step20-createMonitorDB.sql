set mapred.job.queue.name=analytics; /*EOS*/

create view if not exists TARGET.category as select * from SOURCE.category;
create view if not exists TARGET.concept as select * from SOURCE.concept;
create view if not exists TARGET.context as select * from SOURCE.context;
create view if not exists TARGET.country as select * from SOURCE.country;
create view if not exists TARGET.countrygdp as select * from SOURCE.countrygdp;
create view if not exists TARGET.creation_date as select * from SOURCE.creation_date;
create view if not exists TARGET.funder as select * from SOURCE.funder;
create view if not exists TARGET.fundref as select * from SOURCE.fundref;
create view if not exists TARGET.rndexpenditure as select * from SOURCE.rndexpediture;
create view if not exists TARGET.rndgdpexpenditure as select * from SOURCE.rndgdpexpenditure;
create view if not exists TARGET.doctoratestudents as select * from SOURCE.doctoratestudents;
create view if not exists TARGET.totalresearchers as select * from SOURCE.totalresearchers;
create view if not exists TARGET.totalresearchersft as select * from SOURCE.totalresearchersft;
create view if not exists TARGET.hrrst as select * from SOURCE.hrrst;
create view if not exists TARGET.graduatedoctorates as select * from SOURCE.graduatedoctorates;

create table TARGET.result_citations stored as parquet as select * from SOURCE.result_citations orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_references_oc stored as parquet as select * from SOURCE.result_references_oc orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_citations_oc stored as parquet as select * from SOURCE.result_citations_oc orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_classifications stored as parquet as select * from SOURCE.result_classifications orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_apc stored as parquet as select * from SOURCE.result_apc orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_concepts stored as parquet as select * from SOURCE.result_concepts orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_datasources stored as parquet as select * from SOURCE.result_datasources orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_fundercount stored as parquet as select * from SOURCE.result_fundercount orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_gold stored as parquet as select * from SOURCE.result_gold orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_greenoa stored as parquet as select * from SOURCE.result_greenoa orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_languages stored as parquet as select * from SOURCE.result_languages orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_licenses stored as parquet as select * from SOURCE.result_licenses orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.licenses_normalized STORED AS PARQUET as select * from SOURCE.licenses_normalized;

create table TARGET.result_oids stored as parquet as select * from SOURCE.result_oids orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_organization stored as parquet as select * from SOURCE.result_organization orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_peerreviewed stored as parquet as select * from SOURCE.result_peerreviewed orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_pids stored as parquet as select * from SOURCE.result_pids orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_projectcount stored as parquet as select * from SOURCE.result_projectcount orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_projects stored as parquet as select * from SOURCE.result_projects orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_refereed stored as parquet as select * from SOURCE.result_refereed orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_sources stored as parquet as select * from SOURCE.result_sources orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_topics stored as parquet as select * from SOURCE.result_topics orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_fos stored as parquet as select * from SOURCE.result_fos orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_accessroute stored as parquet as select * from SOURCE.result_accessroute orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_orcid stored as parquet as select * from SOURCE.result_orcid orig where exists (select 1 from TARGET.result r where r.id=orig.id);
create table TARGET.indi_pub_publicly_funded stored as parquet as select * from SOURCE.indi_pub_publicly_funded orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_instance stored as parquet as select * from SOURCE.result_instance orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create view TARGET.foo1 as select * from SOURCE.result_result rr where rr.source in (select id from TARGET.result);
create view TARGET.foo2 as select * from SOURCE.result_result rr where rr.target in (select id from TARGET.result);
create table TARGET.result_result STORED AS PARQUET as select distinct * from (select * from TARGET.foo1 union all select * from TARGET.foo2) foufou;
drop view TARGET.foo1;
drop view TARGET.foo2;

-- datasources
create view if not exists TARGET.datasource as select * from SOURCE.datasource;
create view if not exists TARGET.datasource_oids as select * from SOURCE.datasource_oids;
create view if not exists TARGET.datasource_organizations as select * from SOURCE.datasource_organizations;
create view if not exists TARGET.datasource_sources as select * from SOURCE.datasource_sources;

create table TARGET.datasource_results stored as parquet as select id as result, datasource as id from TARGET.result_datasources;

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
create view if not exists TARGET.project_organization_contribution as select * from SOURCE.project_organization_contribution;

create table TARGET.project_results stored as parquet as select id as result, project as id from TARGET.result_projects;

-- indicators
-- Sprint 1 ----
create table TARGET.indi_pub_green_oa stored as parquet as select * from SOURCE.indi_pub_green_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_grey_lit stored as parquet as select * from SOURCE.indi_pub_grey_lit orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_doi_from_crossref stored as parquet as select * from SOURCE.indi_pub_doi_from_crossref orig where exists (select 1 from TARGET.result r where r.id=orig.id);

-- Sprint 2 ----
create table TARGET.indi_result_has_cc_licence stored as parquet as select * from SOURCE.indi_result_has_cc_licence orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_result_has_cc_licence_url stored as parquet as select * from SOURCE.indi_result_has_cc_licence_url orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_has_abstract stored as parquet as select * from SOURCE.indi_pub_has_abstract orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_result_with_orcid stored as parquet as select * from SOURCE.indi_result_with_orcid orig where exists (select 1 from TARGET.result r where r.id=orig.id);

---- Sprint 3 ----
create table TARGET.indi_funded_result_with_fundref stored as parquet as select * from SOURCE.indi_funded_result_with_fundref orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create view TARGET.indi_result_org_collab as select * from SOURCE.indi_result_org_collab;
create view TARGET.indi_result_org_country_collab as select * from SOURCE.indi_result_org_country_collab;
create view TARGET.indi_project_collab_org as select * from SOURCE.indi_project_collab_org;
create view TARGET.indi_project_collab_org_country as select * from SOURCE.indi_project_collab_org_country;
create view TARGET.indi_funder_country_collab as select * from SOURCE.indi_funder_country_collab;
create view TARGET.indi_result_country_collab as select * from SOURCE.indi_result_country_collab;
---- Sprint 4 ----
create table TARGET.indi_pub_diamond stored as parquet as select * from SOURCE.indi_pub_diamond orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_in_transformative stored as parquet as select * from SOURCE.indi_pub_in_transformative orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_closed_other_open stored as parquet as select * from SOURCE.indi_pub_closed_other_open orig where exists (select 1 from TARGET.result r where r.id=orig.id);

---- Sprint 5 ----
create table TARGET.indi_result_no_of_copies stored as parquet as select * from SOURCE.indi_result_no_of_copies orig where exists (select 1 from TARGET.result r where r.id=orig.id);

---- Sprint 6 ----
create table TARGET.indi_pub_hybrid_oa_with_cc stored as parquet as select * from SOURCE.indi_pub_hybrid_oa_with_cc orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_bronze_oa stored as parquet as select * from SOURCE.indi_pub_bronze_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_downloads stored as parquet as select * from SOURCE.indi_pub_downloads orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);

create table TARGET.indi_pub_downloads_datasource stored as parquet as select * from SOURCE.indi_pub_downloads_datasource orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);

create table TARGET.indi_pub_downloads_year stored as parquet as select * from SOURCE.indi_pub_downloads_year orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);

create table TARGET.indi_pub_downloads_datasource_year stored as parquet as select * from SOURCE.indi_pub_downloads_datasource_year orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);

---- Sprint 7 ----
create table TARGET.indi_pub_gold_oa stored as parquet as select * from SOURCE.indi_pub_gold_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_hybrid stored as parquet as select * from SOURCE.indi_pub_hybrid orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create view TARGET.indi_org_fairness as select * from SOURCE.indi_org_fairness;
create view TARGET.indi_org_fairness_pub_pr as select * from SOURCE.indi_org_fairness_pub_pr;
create view TARGET.indi_org_fairness_pub_year as select * from SOURCE.indi_org_fairness_pub_year;
create view TARGET.indi_org_fairness_pub as select * from SOURCE.indi_org_fairness_pub;
create view TARGET.indi_org_fairness_year as select * from SOURCE.indi_org_fairness_year;
create view TARGET.indi_org_findable_year as select * from SOURCE.indi_org_findable_year;
create view TARGET.indi_org_findable as select * from SOURCE.indi_org_findable;
create view TARGET.indi_org_openess as select * from SOURCE.indi_org_openess;
create view TARGET.indi_org_openess_year as select * from SOURCE.indi_org_openess_year;
create table TARGET.indi_pub_has_preprint stored as parquet as select * from SOURCE.indi_pub_has_preprint orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_in_subscribed stored as parquet as select * from SOURCE.indi_pub_in_subscribed orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_result_with_pid stored as parquet as select * from SOURCE.indi_result_with_pid orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_impact_measures stored as parquet as select * from SOURCE.indi_impact_measures orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_pub_interdisciplinarity stored as parquet as select * from SOURCE.indi_pub_interdisciplinarity orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.result_apc_affiliations stored as parquet as select * from SOURCE.result_apc_affiliations orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_is_project_result_after stored as parquet as select * from SOURCE.indi_is_project_result_after orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
create table TARGET.indi_is_funder_plan_s stored as parquet as select * from SOURCE.indi_is_funder_plan_s orig where exists (select 1 from TARGET.result r where r.id=orig.id);
create view TARGET.indi_funder_fairness as select * from SOURCE.indi_funder_fairness;
create view TARGET.indi_funder_openess as select * from SOURCE.indi_funder_openess;
create view TARGET.indi_funder_findable as select * from SOURCE.indi_funder_findable;
create view TARGET.indi_ris_fairness as select * from SOURCE.indi_ris_fairness;
create view TARGET.indi_ris_openess as select * from SOURCE.indi_ris_openess;
create view TARGET.indi_ris_findable as select * from SOURCE.indi_ris_findable;

create table TARGET.indi_pub_green_with_license stored as parquet as select * from SOURCE.indi_pub_green_with_license orig where exists (select 1 from TARGET.result r where r.id=orig.id);
create table TARGET.result_country stored as parquet as select * from SOURCE.result_country orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_result_oa_with_license stored as parquet as select * from SOURCE.indi_result_oa_with_license orig where exists (select 1 from TARGET.result r where r.id=orig.id);
create table TARGET.indi_result_oa_without_license stored as parquet as select * from SOURCE.indi_result_oa_without_license orig where exists (select 1 from TARGET.result r where r.id=orig.id);

create table TARGET.indi_result_under_transformative stored as parquet as select * from SOURCE.indi_result_under_transformative orig where exists (select 1 from TARGET.result r where r.id=orig.id);
