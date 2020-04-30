------------------------------------------------------------
------------------------------------------------------------
-- 9. Datasource table/view and Datasource related tables/views
------------------------------------------------------------
------------------------------------------------------------
-- Datasource table creation & update
-------------------------------------
-- Creating and populating temporary datasource table
DROP TABLE IF EXISTS ${stats_db_name}.datasource_tmp;
create table ${stats_db_name}.datasource_tmp(`id` string, `name` string, `type` string, `dateofvalidation` string, `yearofvalidation` string, `harvested` boolean, `piwik_id` int, `latitude` string, `longitude` string, `websiteurl` string, `compatibility` string) clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
