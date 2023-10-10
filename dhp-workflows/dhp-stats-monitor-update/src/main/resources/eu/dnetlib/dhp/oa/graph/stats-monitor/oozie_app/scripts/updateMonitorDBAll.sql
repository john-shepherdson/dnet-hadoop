DROP TABLE IF EXISTS TARGET.result_new;

create table TARGET.result_new as
    select distinct * from (
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_organization ro where ro.id=r.id and ro.organization in (
             'openorgs____::4d4051b56708688235252f1d8fddb8c1',	--Iscte - Instituto Universitário de Lisboa
             'openorgs____::ab4ac74c35fa5dada770cf08e5110fab'	-- Universidade Católica Portuguesa
        ) )) foo;

INSERT INTO TARGET.result select * from TARGET.result_new;
ANALYZE TABLE TARGET.result_new COMPUTE STATISTICS;

INSERT INTO TARGET.result select * from TARGET.result_new;
ANALYZE TABLE TARGET.result COMPUTE STATISTICS;

INSERT INTO TARGET.result_citations select * from SOURCE.result_citations orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_citations COMPUTE STATISTICS;

INSERT INTO TARGET.result_references_oc select * from SOURCE.result_references_oc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_references_oc COMPUTE STATISTICS;

INSERT INTO TARGET.result_classifications select * from SOURCE.result_classifications orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_classifications COMPUTE STATISTICS;

INSERT INTO TARGET.result_apc select * from SOURCE.result_apc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_apc COMPUTE STATISTICS;

INSERT INTO TARGET.result_concepts select * from SOURCE.result_concepts orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_concepts COMPUTE STATISTICS;

INSERT INTO TARGET.result_datasources select * from SOURCE.result_datasources orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_datasources COMPUTE STATISTICS;

INSERT INTO TARGET.result_fundercount select * from SOURCE.result_fundercount orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_fundercount COMPUTE STATISTICS;

INSERT INTO TARGET.result_gold select * from SOURCE.result_gold orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_gold COMPUTE STATISTICS;

INSERT INTO TARGET.result_greenoa select * from SOURCE.result_greenoa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_greenoa COMPUTE STATISTICS;

INSERT INTO TARGET.result_languages select * from SOURCE.result_languages orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_languages COMPUTE STATISTICS;

INSERT INTO TARGET.result_licenses select * from SOURCE.result_licenses orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_licenses COMPUTE STATISTICS;

INSERT INTO TARGET.result_oids select * from SOURCE.result_oids orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_oids COMPUTE STATISTICS;

INSERT INTO TARGET.result_organization select * from SOURCE.result_organization orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_organization COMPUTE STATISTICS;

INSERT INTO TARGET.result_peerreviewed select * from SOURCE.result_peerreviewed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_peerreviewed COMPUTE STATISTICS;

INSERT INTO TARGET.result_pids select * from SOURCE.result_pids orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_pids COMPUTE STATISTICS;

INSERT INTO TARGET.result_projectcount select * from SOURCE.result_projectcount orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_projectcount COMPUTE STATISTICS;

INSERT INTO TARGET.result_projects select * from SOURCE.result_projects orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_projects COMPUTE STATISTICS;

INSERT INTO TARGET.result_refereed select * from SOURCE.result_refereed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_refereed COMPUTE STATISTICS;

INSERT INTO TARGET.result_sources select * from SOURCE.result_sources orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_sources COMPUTE STATISTICS;

INSERT INTO TARGET.result_topics select * from SOURCE.result_topics orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_topics COMPUTE STATISTICS;

INSERT INTO TARGET.result_fos select * from SOURCE.result_fos orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_fos COMPUTE STATISTICS;

INSERT INTO TARGET.result_accessroute select * from SOURCE.result_accessroute orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.result_accessroute COMPUTE STATISTICS;

create or replace view TARGET.foo1 as select * from SOURCE.result_result rr where rr.source in (select id from TARGET.result_new);
create or replace view TARGET.foo2 as select * from SOURCE.result_result rr where rr.target in (select id from TARGET.result_new);
insert into TARGET.result_result select distinct * from (select * from TARGET.foo1 union all select * from TARGET.foo2) foufou;
drop view TARGET.foo1;
drop view TARGET.foo2;
ANALYZE TABLE TARGET.result_result COMPUTE STATISTICS;


-- indicators
-- Sprint 1 ----
INSERT INTO TARGET.indi_pub_green_oa select * from SOURCE.indi_pub_green_oa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_green_oa COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_grey_lit select * from SOURCE.indi_pub_grey_lit orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_grey_lit COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_doi_from_crossref select * from SOURCE.indi_pub_doi_from_crossref orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_doi_from_crossref COMPUTE STATISTICS;
-- Sprint 2 ----
INSERT INTO TARGET.indi_result_has_cc_licence select * from SOURCE.indi_result_has_cc_licence orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_has_cc_licence COMPUTE STATISTICS;
INSERT INTO TARGET.indi_result_has_cc_licence_url select * from SOURCE.indi_result_has_cc_licence_url orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_has_cc_licence_url COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_has_abstract select * from SOURCE.indi_pub_has_abstract orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_has_abstract COMPUTE STATISTICS;
INSERT INTO TARGET.indi_result_with_orcid select * from SOURCE.indi_result_with_orcid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_with_orcid COMPUTE STATISTICS;
---- Sprint 3 ----
INSERT INTO TARGET.indi_funded_result_with_fundref select * from SOURCE.indi_funded_result_with_fundref orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_funded_result_with_fundref COMPUTE STATISTICS;

---- Sprint 4 ----
INSERT INTO TARGET.indi_pub_diamond select * from SOURCE.indi_pub_diamond orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_diamond COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_in_transformative select * from SOURCE.indi_pub_in_transformative orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_in_transformative COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_closed_other_open select * from SOURCE.indi_pub_closed_other_open orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_closed_other_open COMPUTE STATISTICS;
---- Sprint 5 ----
INSERT INTO TARGET.indi_result_no_of_copies select * from SOURCE.indi_result_no_of_copies orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_no_of_copies COMPUTE STATISTICS;
---- Sprint 6 ----
INSERT INTO TARGET.indi_pub_hybrid_oa_with_cc select * from SOURCE.indi_pub_hybrid_oa_with_cc orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_hybrid_oa_with_cc COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_bronze_oa select * from SOURCE.indi_pub_bronze_oa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_bronze_oa COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads select * from SOURCE.indi_pub_downloads orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads_datasource select * from SOURCE.indi_pub_downloads_datasource orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads_datasource COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads_year select * from SOURCE.indi_pub_downloads_year orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads_year COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_downloads_datasource_year select * from SOURCE.indi_pub_downloads_datasource_year orig where exists (select 1 from TARGET.result_new r where r.id=orig.result_id);
ANALYZE TABLE TARGET.indi_pub_downloads_datasource_year COMPUTE STATISTICS;
---- Sprint 7 ----
INSERT INTO TARGET.indi_pub_gold_oa select * from SOURCE.indi_pub_gold_oa orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_gold_oa COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_hybrid select * from SOURCE.indi_pub_hybrid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_hybrid COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_has_preprint select * from SOURCE.indi_pub_has_preprint orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_has_preprint COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_in_subscribed select * from SOURCE.indi_pub_in_subscribed orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_in_subscribed COMPUTE STATISTICS;
INSERT INTO TARGET.indi_result_with_pid select * from SOURCE.indi_result_with_pid orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_result_with_pid COMPUTE STATISTICS;
INSERT INTO TARGET.indi_impact_measures select * from SOURCE.indi_impact_measures orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_impact_measures COMPUTE STATISTICS;
INSERT INTO TARGET.indi_pub_interdisciplinarity select * from SOURCE.indi_pub_interdisciplinarity orig where exists (select 1 from TARGET.result_new r where r.id=orig.id);
ANALYZE TABLE TARGET.indi_pub_interdisciplinarity COMPUTE STATISTICS;

DROP TABLE IF EXISTS TARGET.result_new;
