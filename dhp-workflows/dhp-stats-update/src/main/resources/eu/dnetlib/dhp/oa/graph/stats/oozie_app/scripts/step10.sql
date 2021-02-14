------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Tables/views from external tables/views (Fundref, Country, CountyGDP, roarmap, rndexpediture)
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW  ${stats_db_name}.fundref AS SELECT * FROM ${external_stats_db_name}.fundref;
CREATE OR REPLACE VIEW  ${stats_db_name}.country AS SELECT * FROM ${external_stats_db_name}.country;
CREATE OR REPLACE VIEW  ${stats_db_name}.countrygdp AS SELECT * FROM ${external_stats_db_name}.countrygdp;
CREATE OR REPLACE VIEW  ${stats_db_name}.roarmap AS SELECT * FROM ${external_stats_db_name}.roarmap;
CREATE OR REPLACE VIEW  ${stats_db_name}.rndexpediture AS SELECT * FROM ${external_stats_db_name}.rndexpediture;
CREATE OR REPLACE VIEW  ${stats_db_name}.context AS SELECT * FROM ${external_stats_db_name}.context;
CREATE OR REPLACE VIEW  ${stats_db_name}.category AS SELECT * FROM ${external_stats_db_name}.category;
CREATE OR REPLACE VIEW  ${stats_db_name}.concept AS SELECT * FROM ${external_stats_db_name}.concept;


------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Creation date of the database
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
create table ${stats_db_name}.creation_date as select date_format(current_date(), 'dd-MM-yyyy') as date;

ANALYZE TABLE ${stats_db_name}.creation_date COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.creation_date COMPUTE STATISTICS FOR COLUMNS;