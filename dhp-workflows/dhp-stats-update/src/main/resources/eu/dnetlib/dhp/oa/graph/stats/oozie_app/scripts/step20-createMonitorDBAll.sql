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
create view if not exists TARGET.rndgdpexpenditure as select * from SOURCE.rndgdpexpenditure;
create view if not exists TARGET.doctoratestudents as select * from SOURCE.doctoratestudents;
create view if not exists TARGET.totalresearchers as select * from SOURCE.totalresearchers;
create view if not exists TARGET.totalresearchersft as select * from SOURCE.totalresearchersft;
create view if not exists TARGET.hrrst as select * from SOURCE.hrrst;
create view if not exists TARGET.graduatedoctorates as select * from SOURCE.graduatedoctorates;

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
             'openorgs____::0ae431b820e4c33db8967fbb2b919150', --University of Helsinki
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
             'openorgs____::6445d7758d3a40c4d997953b6632a368', --National Institute of Informatics (NII)
             'openorgs____::b77c01aa15de3675da34277d48de2ec1', -- Valencia Catholic University Saint Vincent Martyr
             'openorgs____::7fe2f66cdc43983c6b24816bfe9cf6a0', -- Unviersity of Warsaw
             'openorgs____::15e7921fc50d9aa1229a82a84429419e', -- University Of Thessaly
             'openorgs____::11f7919dadc8f8a7251af54bba60c956', -- Technical University of Crete
             'openorgs____::84f0c5f5dbb6daf42748485924efde4b', -- University of Piraeus
             'openorgs____::4ac562f0376fce3539504567649cb373', -- University of Patras
             'openorgs____::3e8d1f8c3f6cd7f418b09f1f58b4873b', -- Aristotle University of Thessaloniki
             'openorgs____::3fcef6e1c469c10f2a84b281372c9814', -- World Bank
             'openorgs____::1698a2eb1885ef8adb5a4a969e745ad3', -- École des Ponts ParisTech
             'openorgs____::e15adb13c4dadd49de4d35c39b5da93a',  -- Nanyang Technological University
             'openorgs____::4b34103bde246228fcd837f5f1bf4212',  -- Autonomous University of Barcelona
             'openorgs____::72ec75fcfc4e0df1a76dc4c49007fceb',	-- McMaster University
             'openorgs____::51c7fc556e46381734a25a6fbc3fd398',	-- University of Modena and Reggio Emilia
             'openorgs____::235d7f9ad18ecd7e6dc62ea4990cb9db',	-- Bilkent University
             'openorgs____::31f2fa9e05b49d4cf40a19c3fed8eb06',	-- Saints Cyril and Methodius University of Skopje
             'openorgs____::db7686f30f22cbe73a4fde872ce812a6', -- University of Milan
             'openorgs____::b8b8ca674452579f3f593d9f5e557483',   -- University College Cork
             'openorgs____::38d7097854736583dde879d12dacafca',	-- Brown University
             'openorgs____::57784c9e047e826fefdb1ef816120d92', --Arts et Métiers ParisTech
             'openorgs____::2530baca8a15936ba2e3297f2bce2e7e',	-- University of Cape Town
             'openorgs____::d11f981828c485cd23d93f7f24f24db1',  -- Technological University Dublin
             'openorgs____::5e6bf8962665cdd040341171e5c631d8',  -- Delft University of Technology
             'openorgs____::846cb428d3f52a445f7275561a7beb5d',  -- University of Manitoba
             'openorgs____::eb391317ed0dc684aa81ac16265de041',	-- Universitat Rovira i Virgili
             'openorgs____::66aa9fc2fceb271423dfabcc38752dc0',  -- Lund University
             'openorgs____::3cff625a4370d51e08624cc586138b2f',	-- IMT Atlantique
             'openorgs____::c0b262bd6eab819e4c994914f9c010e2',   -- National Institute of Geophysics and Volcanology
             'openorgs____::1624ff7c01bb641b91f4518539a0c28a',   -- Vrije Universiteit Amsterdam
             'openorgs____::4d4051b56708688235252f1d8fddb8c1',	 --Iscte - Instituto Universitário de Lisboa
             'openorgs____::ab4ac74c35fa5dada770cf08e5110fab',	-- Universidade Católica Portuguesa
             'openorgs____::4d4051b56708688235252f1d8fddb8c1',	-- Iscte - Instituto Universitário de Lisboa
             'openorgs____::5d55fb216b14691cf68218daf5d78cd9',  -- Munster Technological University
             'openorgs____::0fccc7640f0cb44d5cd1b06b312a06b9',  -- Cardiff University
             'openorgs____::8839b55dae0c84d56fd533f52d5d483a'   -- Leibniz Institute of Ecological Urban and Regional Development
        ) )) foo;

--ANALYZE TABLE TARGET.result COMPUTE STATISTICS;

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
--create view if not exists TARGET.graduatedoctorates as select * from SOURCE.graduatedoctorates;

create table TARGET.result_citations stored as parquet as select * from SOURCE.result_citations orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_citations COMPUTE STATISTICS;

create table TARGET.result_references_oc stored as parquet as select * from SOURCE.result_references_oc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_references_oc COMPUTE STATISTICS;

create table TARGET.result_citations_oc stored as parquet as select * from SOURCE.result_citations_oc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_citations_oc COMPUTE STATISTICS;

create table TARGET.result_classifications stored as parquet as select * from SOURCE.result_classifications orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_classifications COMPUTE STATISTICS;

create table TARGET.result_apc stored as parquet as select * from SOURCE.result_apc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_apc COMPUTE STATISTICS;

create table TARGET.result_concepts stored as parquet as select * from SOURCE.result_concepts orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_concepts COMPUTE STATISTICS;

create table TARGET.result_datasources stored as parquet as select * from SOURCE.result_datasources orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_datasources COMPUTE STATISTICS;

create table TARGET.result_fundercount stored as parquet as select * from SOURCE.result_fundercount orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_fundercount COMPUTE STATISTICS;

create table TARGET.result_gold stored as parquet as select * from SOURCE.result_gold orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_gold COMPUTE STATISTICS;

create table TARGET.result_greenoa stored as parquet as select * from SOURCE.result_greenoa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_greenoa COMPUTE STATISTICS;

create table TARGET.result_languages stored as parquet as select * from SOURCE.result_languages orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_languages COMPUTE STATISTICS;

create table TARGET.result_licenses stored as parquet as select * from SOURCE.result_licenses orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_licenses COMPUTE STATISTICS;

create table TARGET.licenses_normalized STORED AS PARQUET as select * from SOURCE.licenses_normalized;
--ANALYZE TABLE TARGET.licenses_normalized COMPUTE STATISTICS;

create table TARGET.result_oids stored as parquet as select * from SOURCE.result_oids orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_oids COMPUTE STATISTICS;

create table TARGET.result_organization stored as parquet as select * from SOURCE.result_organization orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_organization COMPUTE STATISTICS;

create table TARGET.result_peerreviewed stored as parquet as select * from SOURCE.result_peerreviewed orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_peerreviewed COMPUTE STATISTICS;

create table TARGET.result_pids stored as parquet as select * from SOURCE.result_pids orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_pids COMPUTE STATISTICS;

create table TARGET.result_projectcount stored as parquet as select * from SOURCE.result_projectcount orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_projectcount COMPUTE STATISTICS;

create table TARGET.result_projects stored as parquet as select * from SOURCE.result_projects orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_projects COMPUTE STATISTICS;

create table TARGET.result_refereed stored as parquet as select * from SOURCE.result_refereed orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_refereed COMPUTE STATISTICS;

create table TARGET.result_sources stored as parquet as select * from SOURCE.result_sources orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_sources COMPUTE STATISTICS;

create table TARGET.result_topics stored as parquet as select * from SOURCE.result_topics orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_topics COMPUTE STATISTICS;

create table TARGET.result_fos stored as parquet as select * from SOURCE.result_fos orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_fos COMPUTE STATISTICS;

create table TARGET.result_accessroute stored as parquet as select * from SOURCE.result_accessroute orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_accessroute COMPUTE STATISTICS;

create view TARGET.foo1 as select * from SOURCE.result_result rr where rr.source in (select id from TARGET.result);
create view TARGET.foo2 as select * from SOURCE.result_result rr where rr.target in (select id from TARGET.result);
create table TARGET.result_result STORED AS PARQUET as select distinct * from (select * from TARGET.foo1 union all select * from TARGET.foo2) foufou;
drop view TARGET.foo1;
drop view TARGET.foo2;
--ANALYZE TABLE TARGET.result_result COMPUTE STATISTICS;

-- datasources
create view if not exists TARGET.datasource as select * from SOURCE.datasource;
create view if not exists TARGET.datasource_oids as select * from SOURCE.datasource_oids;
create view if not exists TARGET.datasource_organizations as select * from SOURCE.datasource_organizations;
create view if not exists TARGET.datasource_sources as select * from SOURCE.datasource_sources;

create table TARGET.datasource_results stored as parquet as select id as result, datasource as id from TARGET.result_datasources;
--ANALYZE TABLE TARGET.datasource_results COMPUTE STATISTICS;

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
--ANALYZE TABLE TARGET.project_results COMPUTE STATISTICS;

-- indicators
-- Sprint 1 ----
create table TARGET.indi_pub_green_oa stored as parquet as select * from SOURCE.indi_pub_green_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_green_oa COMPUTE STATISTICS;
create table TARGET.indi_pub_grey_lit stored as parquet as select * from SOURCE.indi_pub_grey_lit orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_grey_lit COMPUTE STATISTICS;
create table TARGET.indi_pub_doi_from_crossref stored as parquet as select * from SOURCE.indi_pub_doi_from_crossref orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_doi_from_crossref COMPUTE STATISTICS;
-- Sprint 2 ----
create table TARGET.indi_result_has_cc_licence stored as parquet as select * from SOURCE.indi_result_has_cc_licence orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_result_has_cc_licence COMPUTE STATISTICS;
create table TARGET.indi_result_has_cc_licence_url stored as parquet as select * from SOURCE.indi_result_has_cc_licence_url orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_result_has_cc_licence_url COMPUTE STATISTICS;
create table TARGET.indi_pub_has_abstract stored as parquet as select * from SOURCE.indi_pub_has_abstract orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_has_abstract COMPUTE STATISTICS;
create table TARGET.indi_result_with_orcid stored as parquet as select * from SOURCE.indi_result_with_orcid orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_result_with_orcid COMPUTE STATISTICS;
---- Sprint 3 ----
create table TARGET.indi_funded_result_with_fundref stored as parquet as select * from SOURCE.indi_funded_result_with_fundref orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_funded_result_with_fundref COMPUTE STATISTICS;
create view TARGET.indi_result_org_collab as select * from SOURCE.indi_result_org_collab;
create view TARGET.indi_result_org_country_collab as select * from SOURCE.indi_result_org_country_collab;
create view TARGET.indi_project_collab_org as select * from SOURCE.indi_project_collab_org;
create view TARGET.indi_project_collab_org_country as select * from SOURCE.indi_project_collab_org_country;
create view TARGET.indi_funder_country_collab as select * from SOURCE.indi_funder_country_collab;
create view TARGET.indi_result_country_collab as select * from SOURCE.indi_result_country_collab;
---- Sprint 4 ----
create table TARGET.indi_pub_diamond stored as parquet as select * from SOURCE.indi_pub_diamond orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_diamond COMPUTE STATISTICS;
create table TARGET.indi_pub_in_transformative stored as parquet as select * from SOURCE.indi_pub_in_transformative orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_in_transformative COMPUTE STATISTICS;
create table TARGET.indi_pub_closed_other_open stored as parquet as select * from SOURCE.indi_pub_closed_other_open orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_closed_other_open COMPUTE STATISTICS;
---- Sprint 5 ----
create table TARGET.indi_result_no_of_copies stored as parquet as select * from SOURCE.indi_result_no_of_copies orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_result_no_of_copies COMPUTE STATISTICS;
---- Sprint 6 ----
create table TARGET.indi_pub_hybrid_oa_with_cc stored as parquet as select * from SOURCE.indi_pub_hybrid_oa_with_cc orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_hybrid_oa_with_cc COMPUTE STATISTICS;
create table TARGET.indi_pub_bronze_oa stored as parquet as select * from SOURCE.indi_pub_bronze_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_bronze_oa COMPUTE STATISTICS;
create table TARGET.indi_pub_downloads stored as parquet as select * from SOURCE.indi_pub_downloads orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
--ANALYZE TABLE TARGET.indi_pub_downloads COMPUTE STATISTICS;
create table TARGET.indi_pub_downloads_datasource stored as parquet as select * from SOURCE.indi_pub_downloads_datasource orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
--ANALYZE TABLE TARGET.indi_pub_downloads_datasource COMPUTE STATISTICS;
create table TARGET.indi_pub_downloads_year stored as parquet as select * from SOURCE.indi_pub_downloads_year orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
--ANALYZE TABLE TARGET.indi_pub_downloads_year COMPUTE STATISTICS;
create table TARGET.indi_pub_downloads_datasource_year stored as parquet as select * from SOURCE.indi_pub_downloads_datasource_year orig where exists (select 1 from TARGET.result r where r.id=orig.result_id);
--ANALYZE TABLE TARGET.indi_pub_downloads_datasource_year COMPUTE STATISTICS;
---- Sprint 7 ----
create table TARGET.indi_pub_gold_oa stored as parquet as select * from SOURCE.indi_pub_gold_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_gold_oa COMPUTE STATISTICS;
create table TARGET.indi_pub_hybrid stored as parquet as select * from SOURCE.indi_pub_hybrid orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_hybrid COMPUTE STATISTICS;
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
--ANALYZE TABLE TARGET.indi_pub_has_preprint COMPUTE STATISTICS;
create table TARGET.indi_pub_in_subscribed stored as parquet as select * from SOURCE.indi_pub_in_subscribed orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_in_subscribed COMPUTE STATISTICS;
create table TARGET.indi_result_with_pid stored as parquet as select * from SOURCE.indi_result_with_pid orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_result_with_pid COMPUTE STATISTICS;
create table TARGET.indi_impact_measures stored as parquet as select * from SOURCE.indi_impact_measures orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_impact_measures COMPUTE STATISTICS;
create table TARGET.indi_pub_interdisciplinarity stored as parquet as select * from SOURCE.indi_pub_interdisciplinarity orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.indi_pub_interdisciplinarity COMPUTE STATISTICS;
create table TARGET.result_apc_affiliations stored as parquet as select * from SOURCE.result_apc_affiliations orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--ANALYZE TABLE TARGET.result_apc_affiliations COMPUTE STATISTICS;
create table TARGET.indi_is_project_result_after stored as parquet as select * from SOURCE.indi_is_project_result_after orig where exists (select 1 from TARGET.result r where r.id=orig.id);
create table TARGET.indi_is_funder_plan_s stored as parquet as select * from SOURCE.indi_is_funder_plan_s orig where exists (select 1 from TARGET.result r where r.id=orig.id);
create view TARGET.indi_funder_fairness as select * from SOURCE.indi_funder_fairness;
create view TARGET.indi_funder_openess as select * from SOURCE.indi_funder_openess;
create view TARGET.indi_funder_findable as select * from SOURCE.indi_funder_findable;
create view TARGET.indi_ris_fairness as select * from SOURCE.indi_ris_fairness;
create view TARGET.indi_ris_openess as select * from SOURCE.indi_ris_openess;
create view TARGET.indi_ris_findable as select * from SOURCE.indi_ris_findable;


