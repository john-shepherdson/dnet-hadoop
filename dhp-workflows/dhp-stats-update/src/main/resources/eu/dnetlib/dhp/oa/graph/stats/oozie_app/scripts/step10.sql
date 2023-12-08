------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Tables/views from external tables/views (Fundref, Country, CountyGDP, roarmap, rndexpediture)
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${stats_db_name}.fundref AS
SELECT *
FROM ${external_stats_db_name}.fundref;

CREATE OR REPLACE VIEW ${stats_db_name}.country AS
SELECT *
FROM ${external_stats_db_name}.country;

CREATE OR REPLACE VIEW ${stats_db_name}.countrygdp AS
SELECT *
FROM ${external_stats_db_name}.countrygdp;

CREATE OR REPLACE VIEW ${stats_db_name}.roarmap AS
SELECT *
FROM ${external_stats_db_name}.roarmap;

CREATE OR REPLACE VIEW ${stats_db_name}.rndexpediture AS
SELECT *
FROM ${external_stats_db_name}.rndexpediture;

CREATE OR REPLACE VIEW ${stats_db_name}.licenses_normalized AS
SELECT *
FROM ${external_stats_db_name}.licenses_normalized;

------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Usage statistics
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
create or replace view ${stats_db_name}.usage_stats as
select * from openaire_prod_usage_stats.usage_stats;

create or replace view ${stats_db_name}.downloads_stats as
select * from openaire_prod_usage_stats.downloads_stats;

create or replace view ${stats_db_name}.pageviews_stats as
select * from openaire_prod_usage_stats.pageviews_stats;

create or replace view ${stats_db_name}.views_stats as
select * from openaire_prod_usage_stats.views_stats;

------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Creation date of the database
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ${stats_db_name}.creation_date purge;

create table ${stats_db_name}.creation_date STORED AS PARQUET as
select date_format(current_date(), 'dd-MM-yyyy') as date;
