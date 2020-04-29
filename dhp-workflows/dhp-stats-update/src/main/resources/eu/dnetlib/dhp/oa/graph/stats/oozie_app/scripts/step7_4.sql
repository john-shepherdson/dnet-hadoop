-- Project table
----------------
-- Creating and populating temporary Project table
DROP TABLE IF EXISTS ${stats_db_name}.project_tmp;
CREATE TABLE ${stats_db_name}.project_tmp (id STRING, acronym STRING, title STRING, funder STRING, funding_lvl0 STRING, funding_lvl1 STRING, funding_lvl2 STRING, ec39 STRING, type STRING, startdate STRING, enddate STRING, start_year STRING, end_year STRING, duration INT, haspubs STRING, numpubs INT, daysforlastpub INT, delayedpubs INT, callidentifier STRING, code STRING) clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
