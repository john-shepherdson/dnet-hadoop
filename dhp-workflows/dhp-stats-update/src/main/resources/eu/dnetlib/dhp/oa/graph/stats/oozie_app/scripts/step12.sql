------------------------------------------------------------------------------------------------------
-- Creating parquet tables from the updated temporary tables and removing unnecessary temporary tables
------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ${stats_db_name}.datasource purge;

CREATE TABLE ${stats_db_name}.datasource stored AS parquet AS
SELECT *
FROM ${stats_db_name}.datasource_tmp;

DROP TABLE IF EXISTS ${stats_db_name}.publication purge;

CREATE TABLE ${stats_db_name}.publication stored AS parquet AS
SELECT *
FROM ${stats_db_name}.publication_tmp;

DROP TABLE IF EXISTS ${stats_db_name}.dataset purge;

CREATE TABLE ${stats_db_name}.dataset stored AS parquet AS
SELECT *
FROM ${stats_db_name}.dataset_tmp;

DROP TABLE IF EXISTS ${stats_db_name}.software purge;

CREATE TABLE ${stats_db_name}.software stored AS parquet AS
SELECT *
FROM ${stats_db_name}.software_tmp;

DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct purge;

CREATE TABLE ${stats_db_name}.otherresearchproduct stored AS parquet AS
SELECT *
FROM ${stats_db_name}.otherresearchproduct_tmp;

DROP TABLE ${stats_db_name}.project_tmp;
DROP TABLE ${stats_db_name}.datasource_tmp;
DROP TABLE ${stats_db_name}.publication_tmp;
DROP TABLE ${stats_db_name}.dataset_tmp;
DROP TABLE ${stats_db_name}.software_tmp;
DROP TABLE ${stats_db_name}.otherresearchproduct_tmp;

----------------------------------------------
-- Re-creating views from final parquet tables
---------------------------------------------

-- Result
CREATE OR REPLACE VIEW ${stats_db_name}.result AS
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.publication
UNION ALL
SELECT *, bestlicence as access_mode
FROM ${stats_db_name}.software
UNION ALL
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.dataset
UNION ALL
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.otherresearchproduct;
