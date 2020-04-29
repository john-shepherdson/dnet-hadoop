----------------------------------------------------
----------------------------------------------------
-- 8. Result table/view and Result related tables/views
----------------------------------------------------
----------------------------------------------------

-- Views on temporary tables that should be re-created in the end
CREATE OR REPLACE VIEW ${stats_db_name}.result as SELECT *, bestlicence as access_mode FROM ${stats_db_name}.publication_tmp UNION ALL SELECT *,bestlicence as access_mode FROM ${stats_db_name}.software_tmp UNION ALL SELECT *,bestlicence as access_mode FROM ${stats_db_name}.dataset_tmp UNION ALL SELECT *,bestlicence as access_mode FROM ${stats_db_name}.otherresearchproduct_tmp;
