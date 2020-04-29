----------------------------------------------------------------
----------------------------------------------------------------
-- Organization table/view and Organization related tables/views
----------------------------------------------------------------
----------------------------------------------------------------
DROP TABLE IF EXISTS ${stats_db_name}.organization;
CREATE TABLE ${stats_db_name}.organization AS SELECT substr(o.id, 4) as id, o.legalname.value as name, o.country.classid as country from ${openaire_db_name}.organization o WHERE o.datainfo.deletedbyinference=false;
