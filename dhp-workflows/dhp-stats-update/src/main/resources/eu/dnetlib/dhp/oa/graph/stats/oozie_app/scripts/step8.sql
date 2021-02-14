-- noinspection SqlNoDataSourceInspectionForFile

------------------------------------------------------------
------------------------------------------------------------
-- Datasource table/view and Datasource related tables/views
------------------------------------------------------------
------------------------------------------------------------
CREATE TABLE ${stats_db_name}.datasource_tmp
(
    `id`               string,
    `name`             STRING,
    `type`             STRING,
    `dateofvalidation` STRING,
    `yearofvalidation` string,
    `harvested`        BOOLEAN,
    `piwik_id`         INT,
    `latitude`         STRING,
    `longitude`        STRING,
    `websiteurl`       STRING,
    `compatibility`    STRING
) CLUSTERED BY (id) INTO 100 buckets stored AS orc tblproperties ('transactional' = 'true');

-- Insert statement that takes into account the piwik_id of the openAIRE graph
INSERT INTO ${stats_db_name}.datasource_tmp
SELECT substr(d1.id, 4)                                          AS id,
       officialname.value                                        AS name,
       datasourcetype.classname                                  AS type,
       dateofvalidation.value                                    AS dateofvalidation,
       date_format(d1.dateofvalidation.value, 'yyyy')            AS yearofvalidation,
       FALSE                                                     AS harvested,
       CASE WHEN d2.piwik_id IS NULL THEN 0 ELSE d2.piwik_id END AS piwik_id,
       d1.latitude.value                                         AS latitude,
       d1.longitude.value                                        AS longitude,
       d1.websiteurl.value                                       AS websiteurl,
       d1.openairecompatibility.classid                          AS compatibility
FROM ${openaire_db_name}.datasource d1
         LEFT OUTER JOIN
     (SELECT id, split(originalidd, '\\:')[1] as piwik_id
      FROM ${openaire_db_name}.datasource
               LATERAL VIEW EXPLODE(originalid) temp AS originalidd
      WHERE originalidd like "piwik:%") AS d2
     ON d1.id = d2.id
WHERE d1.datainfo.deletedbyinference = FALSE;

-- Updating temporary table with everything that is not based on results -> This is done with the following "dual" table.
-- Creating a temporary dual table that will be removed after the following insert
CREATE TABLE ${stats_db_name}.dual
(
    dummy CHAR(1)
);
INSERT INTO ${stats_db_name}.dual
VALUES ('X');
INSERT INTO ${stats_db_name}.datasource_tmp (`id`, `name`, `type`, `dateofvalidation`, `yearofvalidation`, `harvested`,
                                             `piwik_id`, `latitude`, `longitude`, `websiteurl`, `compatibility`)
SELECT 'other',
       'Other',
       'Repository',
       NULL,
       NULL,
       false,
       0,
       NULL,
       NULL,
       NULL,
       'unknown'
FROM ${stats_db_name}.dual
WHERE 'other' not in (SELECT id FROM ${stats_db_name}.datasource_tmp WHERE name = 'Unknown Repository');
DROP TABLE ${stats_db_name}.dual;

UPDATE ${stats_db_name}.datasource_tmp
SET name='Other'
WHERE name = 'Unknown Repository';
UPDATE ${stats_db_name}.datasource_tmp
SET yearofvalidation=null
WHERE yearofvalidation = '-1';

CREATE TABLE ${stats_db_name}.datasource_languages AS
SELECT substr(d.id, 4) AS id, langs.languages AS language
FROM ${openaire_db_name}.datasource d LATERAL VIEW explode(d.odlanguages.value) langs AS languages;

CREATE TABLE ${stats_db_name}.datasource_oids AS
SELECT substr(d.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.datasource d LATERAL VIEW explode(d.originalid) oids AS ids;

CREATE TABLE ${stats_db_name}.datasource_organizations AS
SELECT substr(r.target, 4) AS id, substr(r.source, 4) AS organization
FROM ${openaire_db_name}.relation r
WHERE r.reltype = 'datasourceOrganization'
  and r.datainfo.deletedbyinference = false;

-- datasource sources:
-- where the datasource info have been collected from.
create table if not exists ${stats_db_name}.datasource_sources AS
select substr(d.id, 4) as id, substr(cf.key, 4) as datasource
from ${openaire_db_name}.datasource d lateral view explode(d.collectedfrom) cfrom as cf
where d.datainfo.deletedbyinference = false;

CREATE OR REPLACE VIEW ${stats_db_name}.datasource_results AS
SELECT datasource AS id, id AS result
FROM ${stats_db_name}.result_datasources;

ANALYZE TABLE ${stats_db_name}.datasource_tmp COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.datasource_tmp COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.datasource_languages COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.datasource_languages COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.datasource_oids COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.datasource_oids COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.datasource_organizations COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.datasource_organizations COMPUTE STATISTICS FOR COLUMNS;