--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- 6. Otherresearchproduct table/view and Otherresearchproduct related tables/views
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- Otherresearchproduct temporary table supporting updates
DROP TABLE IF EXISTS ${stats_db_name}.otherresearchproduct_tmp;
CREATE TABLE ${stats_db_name}.otherresearchproduct_tmp (   id STRING,   title STRING,   publisher STRING,   journal STRING,   date STRING,   year STRING,   bestlicence STRING,   embargo_end_date STRING,   delayed BOOLEAN,   authors INT,   source STRING,   abstract BOOLEAN,   type STRING )  clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
