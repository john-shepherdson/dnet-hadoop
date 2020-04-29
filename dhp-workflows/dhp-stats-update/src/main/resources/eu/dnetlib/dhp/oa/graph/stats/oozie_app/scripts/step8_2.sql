-- Views on final tables
CREATE OR REPLACE VIEW ${stats_db_name}.result_datasources as SELECT * FROM ${stats_db_name}.publication_datasources UNION ALL SELECT * FROM ${stats_db_name}.software_datasources UNION ALL SELECT * FROM ${stats_db_name}.dataset_datasources UNION ALL SELECT * FROM ${stats_db_name}.otherresearchproduct_datasources;
