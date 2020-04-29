-- Updating temporary table with everything that is not based on results -> This is done with the following "dual" table. To see if default values are there
-- Creating a temporary dual table that will be removed after the following insert
CREATE TABLE ${stats_db_name}.dual(dummy char(1));
INSERT INTO ${stats_db_name}.dual values('X');
INSERT INTO ${stats_db_name}.datasource_tmp (`id`, `name`, `type`, `dateofvalidation`, `yearofvalidation`, `harvested`, `piwik_id`, `latitude`, `longitude`, `websiteurl`, `compatibility`) 
SELECT 'other', 'Other', 'Repository', NULL, NULL, false, 0, NULL, NULL, NULL, 'unknown' FROM ${stats_db_name}.dual WHERE 'other' not in (SELECT id FROM ${stats_db_name}.datasource_tmp WHERE name='Unknown Repository');
DROP TABLE ${stats_db_name}.dual;
