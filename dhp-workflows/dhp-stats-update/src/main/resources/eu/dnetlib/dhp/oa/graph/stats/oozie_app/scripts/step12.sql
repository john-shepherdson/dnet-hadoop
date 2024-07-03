set mapred.job.queue.name=analytics; /*EOS*/

----------------------------------------------
-- Re-creating views from final parquet tables
---------------------------------------------

-- Result
CREATE OR REPLACE VIEW ${stats_db_name}.result AS
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.publication
UNION ALL
SELECT *, bestlicence as access_mode
FROM ${stats_db_name}.software
UNION ALL
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.dataset
UNION ALL
SELECT *, bestlicence AS access_mode
FROM ${stats_db_name}.otherresearchproduct; /*EOS*/
