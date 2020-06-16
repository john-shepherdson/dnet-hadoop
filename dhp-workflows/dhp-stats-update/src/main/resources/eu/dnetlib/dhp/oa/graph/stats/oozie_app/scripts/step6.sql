------------------------------------------------------
------------------------------------------------------
-- Project table/view and Project related tables/views
------------------------------------------------------
------------------------------------------------------
-- Project_oids Table
DROP TABLE IF EXISTS ${stats_db_name}.project_oids;
CREATE TABLE ${stats_db_name}.project_oids AS SELECT substr(p.id, 4) AS id, oids.ids AS oid FROM ${openaire_db_name}.project p LATERAL VIEW explode(p.originalid) oids AS ids;

-- Project_organizations Table
DROP TABLE IF EXISTS ${stats_db_name}.project_organizations;
CREATE TABLE ${stats_db_name}.project_organizations AS SELECT substr(r.source, 4) AS id, substr(r.target, 4) AS organization from ${openaire_db_name}.relation r WHERE r.reltype='projectOrganization';

-- Project_results Table
DROP TABLE IF EXISTS ${stats_db_name}.project_results;
CREATE TABLE ${stats_db_name}.project_results AS SELECT substr(r.target, 4) AS id, substr(r.source, 4) AS result FROM ${openaire_db_name}.relation r WHERE r.reltype='resultProject' and r.datainfo.deletedbyinference=false;

-- Project table
----------------
-- Creating and populating temporary Project table
DROP TABLE IF EXISTS ${stats_db_name}.project_tmp;
CREATE TABLE ${stats_db_name}.project_tmp (id STRING, acronym STRING, title STRING, funder STRING, funding_lvl0 STRING, funding_lvl1 STRING, funding_lvl2 STRING, ec39 STRING, type STRING, startdate STRING, enddate STRING, start_year STRING, end_year STRING, duration INT, haspubs STRING, numpubs INT, daysforlastpub INT, delayedpubs INT, callidentifier STRING, code STRING) CLUSTERED BY (id) INTO 100 buckets stored AS orc tblproperties('transactional'='true');

INSERT INTO ${stats_db_name}.project_tmp SELECT substr(p.id, 4) AS id, p.acronym.value AS acronym, p.title.value AS title, xpath_string(p.fundingtree[0].value, '//funder/name') AS funder, xpath_string(p.fundingtree[0].value, '//funding_level_0/name') AS funding_lvl0, xpath_string(p.fundingtree[0].value, '//funding_level_1/name') AS funding_lvl1, xpath_string(p.fundingtree[0].value, '//funding_level_2/name') AS funding_lvl2, p.ecsc39.value AS ec39, p.contracttype.classname AS type, p.startdate.value AS startdate, p.enddate.value AS enddate,  date_format(p.startdate.value, 'yyyy') AS start_year, date_format(p.enddate.value, 'yyyy') AS end_year, 0 AS duration, 'no' AS haspubs, 0 AS numpubs, 0 AS daysforlastpub, 0 AS delayedpubs, p.callidentifier.value AS callidentifier, p.code.value AS code FROM ${openaire_db_name}.project p WHERE p.datainfo.deletedbyinference=false;
