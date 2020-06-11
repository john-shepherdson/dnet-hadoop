------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Tables/views from external tables/views (Fundref, Country, CountyGDP, roarmap, rndexpediture)
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${hive_db_name}.fundref AS SELECT * FROM stats_ext.fundref;
CREATE OR REPLACE VIEW ${hive_db_name}.country AS SELECT * FROM stats_ext.country;
CREATE OR REPLACE VIEW ${hive_db_name}.countrygdp AS SELECT * FROM stats_ext.countrygdp;
CREATE OR REPLACE VIEW ${hive_db_name}.roarmap AS SELECT * FROM stats_ext.roarmap;
CREATE OR REPLACE VIEW ${hive_db_name}.rndexpediture AS SELECT * FROM stats_ext.rndexpediture;
