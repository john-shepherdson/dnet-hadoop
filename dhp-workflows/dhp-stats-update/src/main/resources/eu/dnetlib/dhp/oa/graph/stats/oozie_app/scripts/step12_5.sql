-- Dataset temporary table updates
UPDATE ${stats_db_name}.dataset_tmp SET delayed = 'yes' WHERE dataset_tmp.id IN (SELECT distinct r.id FROM stats_wf_db_obs.result r, ${stats_db_name}.project_results pr, ${stats_db_name}.project_tmp p WHERE r.id=pr.result AND pr.id=p.id AND to_date(r.date)-to_date(p.enddate) > 0);
