----------------------------
-- Post processing - Updates
----------------------------

--Datasource temporary table updates
UPDATE ${stats_db_name}.datasource_tmp set harvested ='true' WHERE datasource_tmp.id IN (SELECT DISTINCT d.id FROM ${stats_db_name}.datasource_tmp d, ${stats_db_name}.result_datasources rd where d.id=rd.datasource);
