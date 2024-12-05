-- Views on temporary tables that should be re-created in the end
CREATE TEMPORARY VIEW datasource USING json OPTIONS ( path "${inputpath}/datasource"); /*EOS*/
CREATE TEMPORARY VIEW dataset USING json OPTIONS ( path "${inputpath}/dataset"); /*EOS*/

SET spark.sql.shuffle.partitions = 200; /*EOS*/
CREATE TABLE IF NOT EXISTS ${hiveDbName}.datasource
/* CLUSTERED BY ( id ) SORTED BY ( id ) INO 200 BUCKETS */
STORED AS parquet
AS SELECT * FROM datasource;  /*EOS*/

SET spark.sql.shuffle.partitions = 4000; /*EOS*/
CREATE OR REPLACE TABLE ${hiveDbName}.dataset
/* CLUSTERED BY ( id ) SORTED BY ( id ) INO 4000 BUCKETS */
STORED AS parquet
AS SELECT * FROM dataset;  /*EOS*/
