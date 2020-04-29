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
