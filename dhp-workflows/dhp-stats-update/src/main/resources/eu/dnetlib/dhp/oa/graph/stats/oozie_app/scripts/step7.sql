------------------------------------------------------
------------------------------------------------------
-- 7. Project table/view and Project related tables/views
------------------------------------------------------
------------------------------------------------------
-- Project_oids Table
DROP TABLE IF EXISTS ${hive_db_name}.project_oids;
CREATE TABLE ${hive_db_name}.project_oids AS SELECT substr(p.id, 4) as id, oids.ids as oid from ${hive_source_db_name}.project p LATERAL VIEW explode(p.originalid) oids as ids;
-- Project_organizations Table
DROP TABLE IF EXISTS ${hive_db_name}.project_organizations;
CREATE TABLE ${hive_db_name}.project_organizations AS SELECT substr(r.source, 4) as id, substr(r.target, 4) AS organization from ${hive_source_db_name}.relation r WHERE r.reltype='projectOrganization';
-- Project_results Table
DROP TABLE IF EXISTS ${hive_db_name}.project_results;
CREATE TABLE ${hive_db_name}.project_results AS SELECT substr(r.target, 4) as id, substr(r.source, 4) AS result from ${hive_source_db_name}.relation r WHERE r.reltype='resultProject';

-- Project table
----------------
-- Creating and populating temporary Project table
DROP TABLE IF EXISTS ${hive_db_name}.project_tmp;
CREATE TABLE ${hive_db_name}.project_tmp (id STRING, acronym STRING, title STRING, funder STRING, funding_lvl0 STRING, funding_lvl1 STRING, funding_lvl2 STRING, ec39 STRING, type STRING, startdate STRING, enddate STRING, start_year STRING, end_year STRING, duration INT, haspubs STRING, numpubs INT, daysforlastpub INT, delayedpubs INT, callidentifier STRING, code STRING) clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
INSERT INTO ${hive_db_name}.project_tmp SELECT substr(p.id, 4) as id, p.acronym.value as acronym, p.title.value as title, xpath_string(p.fundingtree[0].value, '//funder/name') as funder, xpath_string(p.fundingtree[0].value, '//funding_level_0/name') as funding_lvl0, xpath_string(p.fundingtree[0].value, '//funding_level_1/name') as funding_lvl1, xpath_string(p.fundingtree[0].value, '//funding_level_2/name') as funding_lvl2, p.ecsc39.value as ec39, p.contracttype.classname as type, p.startdate.value as startdate, p.enddate.value as enddate,  date_format(p.startdate.value, 'yyyy') as start_year, date_format(p.enddate.value, 'yyyy') as end_year, 0 as duration, 'no' as haspubs, 0 as numpubs, 0 as daysforlastpub, 0 as delayedpubs, p.callidentifier.value as callidentifier, p.code.value as code from ${hive_source_db_name}.project p WHERE p.datainfo.deletedbyinference=false;
