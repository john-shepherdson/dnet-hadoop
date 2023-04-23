DROP TABLE IF EXISTS TARGET.result_new;

create table TARGET.result_new stored as parquet as
    select distinct * from (
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_organization ro where ro.id=r.id and ro.organization in (
             'openorgs____::38d7097854736583dde879d12dacafca'	-- Brown University
        ) )) foo;

ANALYZE TABLE TARGET.result_new COMPUTE STATISTICS;

INSERT INTO TARGET.result select * from TARGET.result_new;

INSERT INTO TARGET.result_citations select * from TARGET.result_citations orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_citations COMPUTE STATISTICS;

INSERT INTO TARGET.result_references_oc select * from TARGET.result_references_oc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_references_oc COMPUTE STATISTICS;

INSERT INTO TARGET.result_citations_oc select * from TARGET.result_citations_oc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_citations_oc COMPUTE STATISTICS;

INSERT INTO TARGET.result_classifications select * from TARGET.result_classifications orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_classifications COMPUTE STATISTICS;

INSERT INTO TARGET.result_apc select * from TARGET.result_apc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_apc COMPUTE STATISTICS;

INSERT INTO TARGET.result_concepts select * from TARGET.result_concepts orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_concepts COMPUTE STATISTICS;

INSERT INTO TARGET.result_datasources select * from TARGET.result_datasources orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_datasources COMPUTE STATISTICS;

INSERT INTO TARGET.result_fundercount select * from TARGET.result_fundercount orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_fundercount COMPUTE STATISTICS;

INSERT INTO TARGET.result_gold select * from TARGET.result_gold orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_gold COMPUTE STATISTICS;

INSERT INTO TARGET.result_greenoa select * from TARGET.result_greenoa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_greenoa COMPUTE STATISTICS;

INSERT INTO TARGET.result_languages select * from TARGET.result_languages orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_languages COMPUTE STATISTICS;

INSERT INTO TARGET.result_licenses select * from TARGET.result_licenses orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_licenses COMPUTE STATISTICS;

INSERT INTO TARGET.result_oids select * from TARGET.result_oids orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_oids COMPUTE STATISTICS;

INSERT INTO TARGET.result_organization select * from TARGET.result_organization orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_organization COMPUTE STATISTICS;

INSERT INTO TARGET.result_peerreviewed select * from TARGET.result_peerreviewed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_peerreviewed COMPUTE STATISTICS;

INSERT INTO TARGET.result_pids select * from TARGET.result_pids orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_pids COMPUTE STATISTICS;

INSERT INTO TARGET.result_projectcount select * from TARGET.result_projectcount orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_projectcount COMPUTE STATISTICS;

INSERT INTO TARGET.result_projects select * from TARGET.result_projects orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_projects COMPUTE STATISTICS;

INSERT INTO TARGET.result_refereed select * from TARGET.result_refereed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_refereed COMPUTE STATISTICS;

INSERT INTO TARGET.result_sources select * from TARGET.result_sources orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_sources COMPUTE STATISTICS;

INSERT INTO TARGET.result_topics select * from TARGET.result_topics orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_topics COMPUTE STATISTICS;

INSERT INTO TARGET.result_fos select * from TARGET.result_fos orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_fos COMPUTE STATISTICS;

INSERT INTO TARGET.result select * from TARGET.result_new;
COMPUTE STATS TARGET.result;

INSERT INTO TARGET.result_citations select * from TARGET.result_citations orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_citations;

INSERT INTO TARGET.result_references_oc select * from TARGET.result_references_oc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_references_oc;

INSERT INTO TARGET.result_citations_oc select * from TARGET.result_citations_oc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_citations_oc;

INSERT INTO TARGET.result_classifications select * from TARGET.result_classifications orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_classifications;

INSERT INTO TARGET.result_apc select * from TARGET.result_apc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_apc;

INSERT INTO TARGET.result_concepts select * from TARGET.result_concepts orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_concepts;

INSERT INTO TARGET.result_datasources select * from TARGET.result_datasources orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_datasources;

INSERT INTO TARGET.result_fundercount select * from TARGET.result_fundercount orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_fundercount;

INSERT INTO TARGET.result_gold select * from TARGET.result_gold orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_gold;

INSERT INTO TARGET.result_greenoa select * from TARGET.result_greenoa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_greenoa;

INSERT INTO TARGET.result_languages select * from TARGET.result_languages orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_languages;

INSERT INTO TARGET.result_licenses select * from TARGET.result_licenses orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_licenses;

INSERT INTO TARGET.result_oids select * from TARGET.result_oids orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_oids;

INSERT INTO TARGET.result_organization select * from TARGET.result_organization orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_organization;

INSERT INTO TARGET.result_peerreviewed select * from TARGET.result_peerreviewed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_peerreviewed;

INSERT INTO TARGET.result_pids select * from TARGET.result_pids orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_pids;

INSERT INTO TARGET.result_projectcount select * from TARGET.result_projectcount orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_projectcount;

INSERT INTO TARGET.result_projects select * from TARGET.result_projects orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_projects;

INSERT INTO TARGET.result_refereed select * from TARGET.result_refereed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_refereed;

INSERT INTO TARGET.result_sources select * from TARGET.result_sources orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_sources;

INSERT INTO TARGET.result_topics select * from TARGET.result_topics orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.result_topics;

INSERT INTO TARGET.result_fos select * from TARGET.result_fos orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);

create view TARGET.foo1 as select * from TARGET.result_result rr where rr.source in (select id from TARGET.result_new);
create view TARGET.foo2 as select * from TARGET.result_result rr where rr.target in (select id from TARGET.result_new);
INSERT INTO TARGET.result_result select distinct * from (select * from TARGET.foo1 union all select * from TARGET.foo2) foufou;
drop view TARGET.foo1;
drop view TARGET.foo2;

ANALYZE TABLE TARGET.result_result COMPUTE STATISTICS;


-- indicators
-- Sprint 1 ----
INSERT INTO TARGET.indi_pub_green_oa select * from TARGET.indi_pub_green_oa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_green_oa COMPUTE STATISTICS;

INSERT INTO TARGET.indi_pub_grey_lit select * from TARGET.indi_pub_grey_lit orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_grey_lit COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_doi_from_crossref select * from TARGET.indi_pub_doi_from_crossref orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_doi_from_crossref COMPUTE STATISTICS;
-- Sprint 2 ----
INSERT INTO TARGET.indi_result_has_cc_licence select * from TARGET.indi_result_has_cc_licence orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_has_cc_licence COMPUTE STATISTICS;
INSERT INTO TARGET.indi_result_has_cc_licence_url select * from TARGET.indi_result_has_cc_licence_url orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_has_cc_licence_url COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_has_abstract select * from TARGET.indi_pub_has_abstract orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_has_abstract COMPUTE STATISTICS;
INSERT INTO TARGET.indi_result_with_orcid select * from TARGET.indi_result_with_orcid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_with_orcid COMPUTE STATISTICS;
---- Sprint 3 ----
INSERT INTO TARGET.indi_funded_result_with_fundref select * from TARGET.indi_funded_result_with_fundref orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_funded_result_with_fundref COMPUTE STATISTICS;
---- Sprint 4 ----
INSERT INTO TARGET.indi_pub_diamond select * from TARGET.indi_pub_diamond orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_diamond COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_in_transformative select * from TARGET.indi_pub_in_transformative orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_in_transformative COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_closed_other_open select * from TARGET.indi_pub_closed_other_open orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_closed_other_open COMPUTE STATISTICS;
---- Sprint 5 ----
INSERT INTO TARGET.indi_result_no_of_copies select * from TARGET.indi_result_no_of_copies orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_no_of_copies COMPUTE STATISTICS;
---- Sprint 6 ----
INSERT INTO TARGET.indi_pub_hybrid_oa_with_cc select * from TARGET.indi_pub_hybrid_oa_with_cc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_hybrid_oa_with_cc COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads select * from TARGET.indi_pub_downloads orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads_datasource select * from TARGET.indi_pub_downloads_datasource orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads_datasource COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads_year select * from TARGET.indi_pub_downloads_year orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads_year COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads_datasource_year select * from TARGET.indi_pub_downloads_datasource_year orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads_datasource_year COMPUTE STATISTICS;
---- Sprint 7 ----
INSERT INTO TARGET.indi_pub_gold_oa select * from TARGET.indi_pub_gold_oa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_gold_oa COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_hybrid select * from TARGET.indi_pub_hybrid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_hybrid COMPUTE STATISTICS;

INSERT INTO TARGET.indi_pub_has_preprint select * from TARGET.indi_pub_has_preprint orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_has_preprint COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_in_subscribed select * from TARGET.indi_pub_in_subscribed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_in_subscribed COMPUTE STATISTICS;
INSERT INTO TARGET.indi_result_with_pid select * from TARGET.indi_result_with_pid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_with_pid COMPUTE STATISTICS;
=======
COMPUTE STATS TARGET.indi_pub_green_oa;
INSERT INTO TARGET.indi_pub_grey_lit select * from TARGET.indi_pub_grey_lit orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_grey_lit;
INSERT INTO TARGET.indi_pub_doi_from_crossref select * from TARGET.indi_pub_doi_from_crossref orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_doi_from_crossref;
-- Sprint 2 ----
INSERT INTO TARGET.indi_result_has_cc_licence select * from TARGET.indi_result_has_cc_licence orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_result_has_cc_licence;
INSERT INTO TARGET.indi_result_has_cc_licence_url select * from TARGET.indi_result_has_cc_licence_url orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_result_has_cc_licence_url;
INSERT INTO TARGET.indi_pub_has_abstract select * from TARGET.indi_pub_has_abstract orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_has_abstract;
INSERT INTO TARGET.indi_result_with_orcid select * from TARGET.indi_result_with_orcid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_result_with_orcid;
---- Sprint 3 ----
INSERT INTO TARGET.indi_funded_result_with_fundref select * from TARGET.indi_funded_result_with_fundref orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_funded_result_with_fundref;
---- Sprint 4 ----
INSERT INTO TARGET.indi_pub_diamond select * from TARGET.indi_pub_diamond orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_diamond;
INSERT INTO TARGET.indi_pub_in_transformative select * from TARGET.indi_pub_in_transformative orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_in_transformative;
INSERT INTO TARGET.indi_pub_closed_other_open select * from TARGET.indi_pub_closed_other_open orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_closed_other_open;
---- Sprint 5 ----
INSERT INTO TARGET.indi_result_no_of_copies select * from TARGET.indi_result_no_of_copies orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_result_no_of_copies;
---- Sprint 6 ----
INSERT INTO TARGET.indi_pub_hybrid_oa_with_cc select * from TARGET.indi_pub_hybrid_oa_with_cc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_hybrid_oa_with_cc;
INSERT INTO TARGET.indi_pub_downloads select * from TARGET.indi_pub_downloads orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
COMPUTE STATS TARGET.indi_pub_downloads;
INSERT INTO TARGET.indi_pub_downloads_datasource select * from TARGET.indi_pub_downloads_datasource orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
COMPUTE STATS TARGET.indi_pub_downloads_datasource;
INSERT INTO TARGET.indi_pub_downloads_year select * from TARGET.indi_pub_downloads_year orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
COMPUTE STATS TARGET.indi_pub_downloads_year;
INSERT INTO TARGET.indi_pub_downloads_datasource_year select * from TARGET.indi_pub_downloads_datasource_year orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
COMPUTE STATS TARGET.indi_pub_downloads_datasource_year;
---- Sprint 7 ----
INSERT INTO TARGET.indi_pub_gold_oa select * from TARGET.indi_pub_gold_oa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_gold_oa;
INSERT INTO TARGET.indi_pub_hybrid select * from TARGET.indi_pub_hybrid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_hybrid;

INSERT INTO TARGET.indi_pub_has_preprint select * from TARGET.indi_pub_has_preprint orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_has_preprint;
INSERT INTO TARGET.indi_pub_in_subscribed select * from TARGET.indi_pub_in_subscribed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_pub_in_subscribed;
INSERT INTO TARGET.indi_result_with_pid select * from TARGET.indi_result_with_pid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
COMPUTE STATS TARGET.indi_result_with_pid;

--create table TARGET.indi_datasets_gold_oa stored as parquet as select * from SOURCE.indi_datasets_gold_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--compute stats TARGET.indi_datasets_gold_oa;
--create table TARGET.indi_software_gold_oa stored as parquet as select * from SOURCE.indi_software_gold_oa orig where exists (select 1 from TARGET.result r where r.id=orig.id);
--compute stats TARGET.indi_software_gold_oa;
DROP TABLE TARGET.result_new;
