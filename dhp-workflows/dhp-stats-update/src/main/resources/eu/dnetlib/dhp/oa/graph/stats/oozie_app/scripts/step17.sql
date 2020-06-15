------------------------------------------------------
------------------------------------------------------
-- Impala table statistics - Needed to make the tables
-- visible for impala
------------------------------------------------------
------------------------------------------------------

COMPUTE STATS dataset;
COMPUTE STATS dataset_citations;
COMPUTE STATS dataset_classifications;
COMPUTE STATS dataset_concepts;
COMPUTE STATS dataset_datasources;
COMPUTE STATS dataset_languages;
COMPUTE STATS dataset_oids;
COMPUTE STATS dataset_pids;
COMPUTE STATS dataset_topics;
COMPUTE STATS datasource;
COMPUTE STATS datasource_languages;
COMPUTE STATS datasource_oids;
COMPUTE STATS datasource_organizations;
COMPUTE STATS numbers_country;
COMPUTE STATS organization;
COMPUTE STATS otherresearchproduct;
COMPUTE STATS otherresearchproduct_citations;
COMPUTE STATS otherresearchproduct_classifications;
COMPUTE STATS otherresearchproduct_concepts;
COMPUTE STATS otherresearchproduct_datasources;
COMPUTE STATS otherresearchproduct_languages;
COMPUTE STATS otherresearchproduct_oids;
COMPUTE STATS otherresearchproduct_pids;
COMPUTE STATS otherresearchproduct_topics;
COMPUTE STATS project;
COMPUTE STATS project_oids;
COMPUTE STATS project_organizations;
COMPUTE STATS project_results;
COMPUTE STATS publication;
COMPUTE STATS publication_citations;
COMPUTE STATS publication_classifications;
COMPUTE STATS publication_concepts;
COMPUTE STATS publication_datasources;
COMPUTE STATS publication_languages;
COMPUTE STATS publication_oids;
COMPUTE STATS publication_pids;
COMPUTE STATS publication_topics;
COMPUTE STATS result_organization;
COMPUTE STATS result_projects;
COMPUTE STATS software;
COMPUTE STATS software_citations;
COMPUTE STATS software_classifications;
COMPUTE STATS software_concepts;
COMPUTE STATS software_datasources;
COMPUTE STATS software_languages;
COMPUTE STATS software_oids;
COMPUTE STATS software_pids;
