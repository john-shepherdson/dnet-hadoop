------------------------------------------------------
------------------------------------------------------
-- 7. Project table/view and Project related tables/views
------------------------------------------------------
------------------------------------------------------
-- Project_oids Table
DROP TABLE IF EXISTS ${stats_db_name}.project_oids;
CREATE TABLE ${stats_db_name}.project_oids AS SELECT substr(p.id, 4) as id, oids.ids as oid from ${openaire_db_name}.project p LATERAL VIEW explode(p.originalid) oids as ids;
