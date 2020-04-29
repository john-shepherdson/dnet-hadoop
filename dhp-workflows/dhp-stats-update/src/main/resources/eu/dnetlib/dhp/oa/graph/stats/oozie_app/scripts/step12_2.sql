-- Project temporary table update and final project table creation with final updates that can not be applied to ORC tables
UPDATE ${stats_db_name}.project_tmp SET haspubs='yes' WHERE project_tmp.id IN (SELECT pr.id FROM ${stats_db_name}.project_results pr, ${stats_db_name}.result r WHERE pr.result=r.id AND r.type='publication');
