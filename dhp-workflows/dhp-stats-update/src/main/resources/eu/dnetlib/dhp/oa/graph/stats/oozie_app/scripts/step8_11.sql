DROP TABLE IF EXISTS ${stats_db_name}.result_projects;
CREATE TABLE ${stats_db_name}.result_projects AS select pr.result as id, pr.id as project, datediff(p.enddate, p.startdate) as daysfromend from ${stats_db_name}.result r join ${stats_db_name}.project_results pr on r.id=pr.result join ${stats_db_name}.project_tmp p on p.id=pr.id;
