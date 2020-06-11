------------------------------------------------------------------------------------------------------
-- Creating parquet tables from the updated temporary tables and removing unnecessary temporary tables
------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ${hive_db_name}.datasource;
CREATE TABLE ${hive_db_name}.datasource stored as parquet as select * from ${hive_db_name}.datasource_tmp;

DROP TABLE IF EXISTS  ${hive_db_name}.publication;
CREATE TABLE ${hive_db_name}.publication stored as parquet as select * from ${hive_db_name}.publication_tmp;

DROP TABLE IF EXISTS ${hive_db_name}.dataset;
CREATE TABLE ${hive_db_name}.dataset stored as parquet as select * from ${hive_db_name}.dataset_tmp;

DROP TABLE IF EXISTS ${hive_db_name}.software;
CREATE TABLE ${hive_db_name}.software stored as parquet as select * from ${hive_db_name}.software_tmp;

DROP TABLE IF EXISTS ${hive_db_name}.otherresearchproduct;
CREATE TABLE ${hive_db_name}.otherresearchproduct stored as parquet as select * from ${hive_db_name}.otherresearchproduct_tmp;

DROP TABLE ${hive_db_name}.project_tmp;
DROP TABLE ${hive_db_name}.datasource_tmp;
DROP TABLE ${hive_db_name}.publication_tmp;
DROP TABLE ${hive_db_name}.dataset_tmp;
DROP TABLE ${hive_db_name}.software_tmp;
DROP TABLE ${hive_db_name}.otherresearchproduct_tmp;

----------------------------------------------
-- Re-creating views from final parquet tables
---------------------------------------------

-- Result
CREATE OR REPLACE VIEW ${hive_db_name}.result as SELECT *, bestlicence as access_mode FROM ${hive_db_name}.publication UNION ALL SELECT *, bestlicence as access_mode FROM ${hive_db_name}.software UNION ALL SELECT *, bestlicence as access_mode FROM ${hive_db_name}.dataset UNION ALL SELECT *, bestlicence as access_mode FROM ${hive_db_name}.otherresearchproduct;

-- cleanup
drop view if exists ${hive_db_name}.delayedpubs;
drop view if exists ${hive_db_name}.project_pub_count;
drop view if exists ${hive_db_name}.delayedpubs;
drop view if exists ${hive_db_name}.project_results_publication;

 CREATE TABLE ${hive_db_name}.numbers_country AS SELECT org.country AS country, count(distinct rd.datasource) AS datasources, count(distinct r.id) AS publications FROM ${hive_db_name}.result r, ${hive_db_name}.result_datasources rd, ${hive_db_name}.datasource d, ${hive_db_name}.datasource_organizations dor, ${hive_db_name}.organization org WHERE r.id=rd.id AND rd.datasource=d.id AND d.id=dor.id AND dor.organization=org.id AND r.type='publication' AND r.bestlicence='Open Access' GROUP BY org.country;
