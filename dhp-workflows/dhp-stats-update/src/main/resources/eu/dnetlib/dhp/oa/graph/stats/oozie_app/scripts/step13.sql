------------------------------------------------------------------------------------------------------
-- Creating parquet tables from the updated temporary tables and removing unnecessary temporary tables
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${stats_db_name}.datasource;
CREATE TABLE ${stats_db_name}.datasource stored as parquet as select * from ${stats_db_name}.datasource_tmp;

DROP TABLE IF EXISTS  ${stats_db_name}.publication;
CREATE TABLE ${stats_db_name}.publication stored as parquet as select * from ${stats_db_name}.publication_tmp;

DROP TABLE IF EXISTS ${stats_db_name}.dataset;
CREATE TABLE ${stats_db_name}.dataset stored as parquet as select * from ${stats_db_name}.dataset_tmp;

DROP TABLE IF EXISTS ${stats_db_name}.software;
CREATE TABLE ${stats_db_name}.software stored as parquet as select * from ${stats_db_name}.software_tmp;

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct;
CREATE TABLE ${stats_db_name}.otherresearchproduct stored as parquet as select * from ${stats_db_name}.otherresearchproduct_tmp;

DROP TABLE ${stats_db_name}.project_tmp;
DROP TABLE ${stats_db_name}.datasource_tmp;
DROP TABLE ${stats_db_name}.publication_tmp;
DROP TABLE ${stats_db_name}.dataset_tmp;
DROP TABLE ${stats_db_name}.software_tmp;
DROP TABLE ${stats_db_name}.otherresearchproduct_tmp;

