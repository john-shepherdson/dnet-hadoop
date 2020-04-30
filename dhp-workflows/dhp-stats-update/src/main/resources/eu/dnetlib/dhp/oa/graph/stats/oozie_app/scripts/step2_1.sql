--------------------------------------------------------------
--------------------------------------------------------------
-- 2. Publication table/view and Publication related tables/views
--------------------------------------------------------------
--------------------------------------------------------------

-- Publication temporary table
DROP TABLE IF EXISTS ${stats_db_name}.publication_tmp;

CREATE TABLE ${stats_db_name}.publication_tmp (id STRING,   title STRING,   publisher STRING,   journal STRING,   date STRING,   year STRING,   bestlicence STRING,   embargo_end_date STRING,   delayed BOOLEAN,   authors INT,   source STRING,   abstract BOOLEAN,   type STRING )  clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
