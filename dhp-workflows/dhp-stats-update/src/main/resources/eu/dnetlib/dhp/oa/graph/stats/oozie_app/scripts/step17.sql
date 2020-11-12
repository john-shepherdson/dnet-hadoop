------------------------------------------------------
------------------------------------------------------
-- Shadow schema table exchange
------------------------------------------------------
------------------------------------------------------

-- Dropping old views
DROP VIEW IF EXISTS ${stats_db_shadow_name}.category;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.concept;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.context;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.country;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.countrygdp;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.creation_date;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_citations;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_classifications;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_concepts;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_datasources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_languages;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_licenses;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_oids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_pids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_refereed;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_sources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.dataset_topics;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.datasource;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.datasource_languages;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.datasource_oids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.datasource_organizations;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.datasource_results;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.datasource_sources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.funder;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.fundref;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.numbers_country;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.organization;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.organization_datasources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.organization_pids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.organization_projects;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.organization_sources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_citations;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_classifications;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_concepts;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_datasources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_languages;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_licenses;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_oids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_pids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_refereed;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_sources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.otherresearchproduct_topics;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.project;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.project_oids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.project_organizations;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.project_results;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.project_resultcount;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.project_results_publication;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_citations;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_classifications;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_concepts;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_datasources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_languages;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_licenses;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_oids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_pids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_refereed;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_sources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.publication_topics;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_affiliated_country;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_citations;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_classifications;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_concepts;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_datasources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_deposited_country;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_fundercount;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_gold;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_greenoa;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_languages;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_licenses;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_oids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_organization;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_peerreviewed;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_pids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_projectcount;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_projects;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_refereed;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_sources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.result_topics;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.rndexpediture;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.roarmap;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_citations;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_classifications;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_concepts;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_datasources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_languages;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_licenses;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_oids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_pids;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_refereed;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_sources;
DROP VIEW IF EXISTS ${stats_db_shadow_name}.software_topics;


-- Creating the shadow database, in case it doesn't exist
CREATE database IF NOT EXISTS ${stats_db_shadow_name};

-- Creating new views
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.category AS SELECT * FROM ${stats_db_name}.category;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.concept AS SELECT * FROM ${stats_db_name}.concept;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.context AS SELECT * FROM ${stats_db_name}.context;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.country AS SELECT * FROM ${stats_db_name}.country;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.countrygdp AS SELECT * FROM ${stats_db_name}.countrygdp;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.creation_date AS SELECT * FROM ${stats_db_name}.creation_date;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset AS SELECT * FROM ${stats_db_name}.dataset;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_citations AS SELECT * FROM ${stats_db_name}.dataset_citations;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_classifications AS SELECT * FROM ${stats_db_name}.dataset_classifications;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_concepts AS SELECT * FROM ${stats_db_name}.dataset_concepts;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_datasources AS SELECT * FROM ${stats_db_name}.dataset_datasources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_languages AS SELECT * FROM ${stats_db_name}.dataset_languages;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_licenses AS SELECT * FROM ${stats_db_name}.dataset_licenses;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_oids AS SELECT * FROM ${stats_db_name}.dataset_oids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_pids AS SELECT * FROM ${stats_db_name}.dataset_pids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_refereed AS SELECT * FROM ${stats_db_name}.dataset_refereed;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_sources AS SELECT * FROM ${stats_db_name}.dataset_sources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.dataset_topics AS SELECT * FROM ${stats_db_name}.dataset_topics;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.datasource AS SELECT * FROM ${stats_db_name}.datasource;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.datasource_languages AS SELECT * FROM ${stats_db_name}.datasource_languages;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.datasource_oids AS SELECT * FROM ${stats_db_name}.datasource_oids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.datasource_organizations AS SELECT * FROM ${stats_db_name}.datasource_organizations;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.datasource_results AS SELECT * FROM ${stats_db_name}.datasource_results;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.datasource_sources AS SELECT * FROM ${stats_db_name}.datasource_sources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.funder AS SELECT * FROM ${stats_db_name}.funder;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.fundref AS SELECT * FROM ${stats_db_name}.fundref;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.numbers_country AS SELECT * FROM ${stats_db_name}.numbers_country;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.organization AS SELECT * FROM ${stats_db_name}.organization;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.organization_datasources AS SELECT * FROM ${stats_db_name}.organization_datasources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.organization_pids AS SELECT * FROM ${stats_db_name}.organization_pids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.organization_projects AS SELECT * FROM ${stats_db_name}.organization_projects;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.organization_sources AS SELECT * FROM ${stats_db_name}.organization_sources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct AS SELECT * FROM ${stats_db_name}.otherresearchproduct;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_citations AS SELECT * FROM ${stats_db_name}.otherresearchproduct_citations;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_classifications AS SELECT * FROM ${stats_db_name}.otherresearchproduct_classifications;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_concepts AS SELECT * FROM ${stats_db_name}.otherresearchproduct_concepts;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_datasources AS SELECT * FROM ${stats_db_name}.otherresearchproduct_datasources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_languages AS SELECT * FROM ${stats_db_name}.otherresearchproduct_languages;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_licenses AS SELECT * FROM ${stats_db_name}.otherresearchproduct_licenses;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_oids AS SELECT * FROM ${stats_db_name}.otherresearchproduct_oids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_pids AS SELECT * FROM ${stats_db_name}.otherresearchproduct_pids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_refereed AS SELECT * FROM ${stats_db_name}.otherresearchproduct_refereed;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_sources AS SELECT * FROM ${stats_db_name}.otherresearchproduct_sources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.otherresearchproduct_topics AS SELECT * FROM ${stats_db_name}.otherresearchproduct_topics;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.project AS SELECT * FROM ${stats_db_name}.project;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.project_oids AS SELECT * FROM ${stats_db_name}.project_oids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.project_organizations AS SELECT * FROM ${stats_db_name}.project_organizations;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.project_results AS SELECT * FROM ${stats_db_name}.project_results;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.project_resultcount AS SELECT * FROM ${stats_db_name}.project_resultcount;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.project_results_publication AS SELECT * FROM ${stats_db_name}.project_results_publication;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication AS SELECT * FROM ${stats_db_name}.publication;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_citations AS SELECT * FROM ${stats_db_name}.publication_citations;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_classifications AS SELECT * FROM ${stats_db_name}.publication_classifications;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_concepts AS SELECT * FROM ${stats_db_name}.publication_concepts;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_datasources AS SELECT * FROM ${stats_db_name}.publication_datasources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_languages AS SELECT * FROM ${stats_db_name}.publication_languages;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_licenses AS SELECT * FROM ${stats_db_name}.publication_licenses;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_oids AS SELECT * FROM ${stats_db_name}.publication_oids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_pids AS SELECT * FROM ${stats_db_name}.publication_pids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_refereed AS SELECT * FROM ${stats_db_name}.publication_refereed;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_sources AS SELECT * FROM ${stats_db_name}.publication_sources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.publication_topics AS SELECT * FROM ${stats_db_name}.publication_topics;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result AS SELECT * FROM ${stats_db_name}.result;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_affiliated_country AS SELECT * FROM ${stats_db_name}.result_affiliated_country;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_citations AS SELECT * FROM ${stats_db_name}.result_citations;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_classifications AS SELECT * FROM ${stats_db_name}.result_classifications;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_concepts AS SELECT * FROM ${stats_db_name}.result_concepts;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_datasources AS SELECT * FROM ${stats_db_name}.result_datasources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_deposited_country AS SELECT * FROM ${stats_db_name}.result_deposited_country;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_fundercount AS SELECT * FROM ${stats_db_name}.result_fundercount;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_gold AS SELECT * FROM ${stats_db_name}.result_gold;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_greenoa AS SELECT * FROM ${stats_db_name}.result_greenoa;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_languages AS SELECT * FROM ${stats_db_name}.result_languages;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_licenses AS SELECT * FROM ${stats_db_name}.result_licenses;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_oids AS SELECT * FROM ${stats_db_name}.result_oids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_organization AS SELECT * FROM ${stats_db_name}.result_organization;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_peerreviewed AS SELECT * FROM ${stats_db_name}.result_peerreviewed;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_pids AS SELECT * FROM ${stats_db_name}.result_pids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_projectcount AS SELECT * FROM ${stats_db_name}.result_projectcount;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_projects AS SELECT * FROM ${stats_db_name}.result_projects;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_refereed AS SELECT * FROM ${stats_db_name}.result_refereed;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_sources AS SELECT * FROM ${stats_db_name}.result_sources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.result_topics AS SELECT * FROM ${stats_db_name}.result_topics;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.rndexpediture AS SELECT * FROM ${stats_db_name}.rndexpediture;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.roarmap AS SELECT * FROM ${stats_db_name}.roarmap;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software AS SELECT * FROM ${stats_db_name}.software;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_citations AS SELECT * FROM ${stats_db_name}.software_citations;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_classifications AS SELECT * FROM ${stats_db_name}.software_classifications;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_concepts AS SELECT * FROM ${stats_db_name}.software_concepts;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_datasources AS SELECT * FROM ${stats_db_name}.software_datasources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_languages AS SELECT * FROM ${stats_db_name}.software_languages;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_licenses AS SELECT * FROM ${stats_db_name}.software_licenses;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_oids AS SELECT * FROM ${stats_db_name}.software_oids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_pids AS SELECT * FROM ${stats_db_name}.software_pids;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_refereed AS SELECT * FROM ${stats_db_name}.software_refereed;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_sources AS SELECT * FROM ${stats_db_name}.software_sources;
CREATE VIEW IF NOT EXISTS ${stats_db_shadow_name}.software_topics AS SELECT * FROM ${stats_db_name}.software_topics;
