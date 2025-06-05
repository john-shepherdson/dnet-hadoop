set mapred.job.queue.name=analytics; /*EOS*/

----------------------------------------------------------------
----------------------------------------------------------------
-- Post processing - Updates on main tables
----------------------------------------------------------------
----------------------------------------------------------------

CREATE OR REPLACE VIEW ${stats_db_name}.project_results_publication AS
SELECT result_projects.id          AS result,
       result_projects.project     AS project_results,
       result.date                 as resultdate,
       project.enddate             as projectenddate,
       result_projects.daysfromend AS daysfromend
FROM ${stats_db_name}.result_projects,
     ${stats_db_name}.result,
     ${stats_db_name}.project
WHERE result_projects.id = result.id
  AND result.type = 'publication'
  AND project.id = result_projects.project; /*EOS*/