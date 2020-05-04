-- Project_results Table
DROP TABLE IF EXISTS ${stats_db_name}.project_results;
CREATE TABLE ${stats_db_name}.project_results AS SELECT substr(r.target, 4) as id, substr(r.source, 4) AS result from ${openaire_db_name}.relation r WHERE r.reltype='resultProject';
