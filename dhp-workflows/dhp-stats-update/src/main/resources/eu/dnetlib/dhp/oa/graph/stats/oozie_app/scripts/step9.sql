------------------------------------------------------------
------------------------------------------------------------
-- 9. Datasource table/view and Datasource related tables/views
------------------------------------------------------------
------------------------------------------------------------
-- Datasource table creation & update
-------------------------------------
-- Creating and populating temporary datasource table
DROP TABLE IF EXISTS ${stats_db_name}.datasource_tmp;
CREATE TABLE ${stats_db_name}.datasource_tmp(`id` string, `name` STRING, `type` STRING, `dateofvalidation` STRING, `yearofvalidation` string, `harvested` BOOLEAN, `piwik_id` INT, `latitude` STRING, `longitude`STRING, `websiteurl` STRING, `compatibility` STRING) CLUSTERED BY (id) INTO 100 buckets stored AS orc tblproperties('transactional'='true');

INSERT INTO ${stats_db_name}.datasource_tmp SELECT substr(d.id, 4) AS id, officialname.value AS name, datasourcetype.classname AS type, dateofvalidation.value AS dateofvalidation, date_format(d.dateofvalidation.value,'yyyy') AS yearofvalidation, FALSE AS harvested, 0 AS piwik_id, d.latitude.value AS latitude, d.longitude.value AS longitude, d.websiteurl.value AS websiteurl, d.openairecompatibility.classid AS compatibility
FROM ${openaire_db_name}.datasource d
WHERE d.datainfo.deletedbyinference=FALSE;

-- Updating temporary table with everything that is not based on results -> This is done with the following "dual" table.
-- Creating a temporary dual table that will be removed after the following insert
CREATE TABLE ${stats_db_name}.dual(dummy CHAR(1));
INSERT INTO ${stats_db_name}.dual VALUES('X');
INSERT INTO ${stats_db_name}.datasource_tmp (`id`, `name`, `type`, `dateofvalidation`, `yearofvalidation`, `harvested`, `piwik_id`, `latitude`, `longitude`, `websiteurl`, `compatibility`) 
SELECT 'other', 'Other', 'Repository', NULL, NULL, false, 0, NULL, NULL, NULL, 'unknown' FROM ${stats_db_name}.dual WHERE 'other' not in (SELECT id FROM ${stats_db_name}.datasource_tmp WHERE name='Unknown Repository');
DROP TABLE ${stats_db_name}.dual;

UPDATE ${stats_db_name}.datasource_tmp SET name='Other' WHERE name='Unknown Repository';
UPDATE ${stats_db_name}.datasource_tmp SET yearofvalidation=null WHERE yearofvalidation='-1';

DROP TABLE IF EXISTS ${stats_db_name}.datasource_languages;
CREATE TABLE ${stats_db_name}.datasource_languages AS SELECT substr(d.id, 4) AS id, langs.languages AS language FROM ${openaire_db_name}.datasource d LATERAL VIEW explode(d.odlanguages.value) langs AS languages;

DROP TABLE IF EXISTS ${stats_db_name}.datasource_oids;
CREATE TABLE ${stats_db_name}.datasource_oids AS SELECT substr(d.id, 4) AS id, oids.ids AS oid FROM ${openaire_db_name}.datasource d LATERAL VIEW explode(d.originalid) oids AS ids;

DROP TABLE IF EXISTS ${stats_db_name}.datasource_organizations;
CREATE TABLE ${stats_db_name}.datasource_organizations AS SELECT substr(r.target, 4) AS id, substr(r.source, 4) AS organization FROM ${openaire_db_name}.relation r WHERE r.reltype='datasourceOrganization';

CREATE OR REPLACE VIEW ${stats_db_name}.datasource_results AS SELECT datasource AS id, id AS result FROM ${stats_db_name}.result_datasources;
