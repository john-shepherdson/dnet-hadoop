------------------------------------------------------------------------------------------------------
-- Creating parquet tables from the updated temporary tables and removing unnecessary temporary tables
------------------------------------------------------------------------------------------------------
CREATE TABLE ${stats_db_name}.datasource stored AS parquet AS
SELECT *
FROM ${stats_db_name}.datasource_tmp;

CREATE TABLE ${stats_db_name}.publication stored AS parquet AS
SELECT *
FROM ${stats_db_name}.publication_tmp;

CREATE TABLE ${stats_db_name}.dataset stored AS parquet AS
SELECT *
FROM ${stats_db_name}.dataset_tmp;

CREATE TABLE ${stats_db_name}.software stored AS parquet AS
SELECT *
FROM ${stats_db_name}.software_tmp;

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


-------------------------------------------------------------------------------
-- To see with Antonis if the following is needed and where it should be placed
-------------------------------------------------------------------------------
CREATE TABLE ${stats_db_name}.numbers_country AS
SELECT org.country AS country, count(distinct rd.datasource) AS datasources, count(distinct r.id) AS publications
FROM ${stats_db_name}.result r,
     ${stats_db_name}.result_datasources rd,
     ${stats_db_name}.datasource d,
     ${stats_db_name}.datasource_organizations dor,
     ${stats_db_name}.organization org
WHERE r.id = rd.id
  AND rd.datasource = d.id
  AND d.id = dor.id
  AND dor.organization = org.id
  AND r.type = 'publication'
  AND r.bestlicence = 'Open Access'
GROUP BY org.country;

-- ANALYZE TABLE ${stats_db_name}.datasource COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.datasource COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.publication COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.publication COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.dataset COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.dataset COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.software COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.software COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.otherresearchproduct COMPUTE STATISTICS FOR COLUMNS;
-- ANALYZE TABLE ${stats_db_name}.numbers_country COMPUTE STATISTICS;
-- ANALYZE TABLE ${stats_db_name}.numbers_country COMPUTE STATISTICS FOR COLUMNS;