------------------------------------------------------
------------------------------------------------------
-- Project table/view and Project related tables/views
------------------------------------------------------
------------------------------------------------------
CREATE TABLE ${stats_db_name}.project_oids AS
SELECT substr(p.id, 4) AS id, oids.ids AS oid
FROM ${openaire_db_name}.project p LATERAL VIEW explode(p.originalid) oids AS ids;
CREATE TABLE ${stats_db_name}.project_organizations AS
SELECT substr(r.source, 4) AS id, substr(r.target, 4) AS organization
from ${openaire_db_name}.relation r
WHERE r.reltype = 'projectOrganization'
  and r.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.project_results AS
SELECT substr(r.target, 4) AS id, substr(r.source, 4) AS result
FROM ${openaire_db_name}.relation r
WHERE r.reltype = 'resultProject'
  and r.datainfo.deletedbyinference = false;

CREATE TABLE ${stats_db_name}.project_tmp
(
    id             STRING,
    acronym        STRING,
    title          STRING,
    funder         STRING,
    funding_lvl0   STRING,
    funding_lvl1   STRING,
    funding_lvl2   STRING,
    ec39           STRING,
    type           STRING,
    startdate      STRING,
    enddate        STRING,
    start_year     INT,
    end_year       INT,
    duration       INT,
    haspubs        STRING,
    numpubs        INT,
    daysforlastpub INT,
    delayedpubs    INT,
    callidentifier STRING,
    code           STRING
) CLUSTERED BY (id) INTO 100 buckets stored AS orc tblproperties ('transactional' = 'true');

INSERT INTO ${stats_db_name}.project_tmp
SELECT substr(p.id, 4)                                                 AS id,
       p.acronym.value                                                 AS acronym,
       p.title.value                                                   AS title,
       xpath_string(p.fundingtree[0].value, '//funder/name')           AS funder,
       xpath_string(p.fundingtree[0].value, '//funding_level_0/name')  AS funding_lvl0,
       xpath_string(p.fundingtree[0].value, '//funding_level_1/name')  AS funding_lvl1,
       xpath_string(p.fundingtree[0].value, '//funding_level_2/name')  AS funding_lvl2,
       p.ecsc39.value                                                  AS ec39,
       p.contracttype.classname                                        AS type,
       p.startdate.value                                               AS startdate,
       p.enddate.value                                                 AS enddate,
       year(p.startdate.value)                                         AS start_year,
       year(p.enddate.value)                                           AS end_year,
       CAST(MONTHS_BETWEEN(p.enddate.value, p.startdate.value) AS INT) AS duration,
       'no'                                                            AS haspubs,
       0                                                               AS numpubs,
       0                                                               AS daysforlastpub,
       0                                                               AS delayedpubs,
       p.callidentifier.value                                          AS callidentifier,
       p.code.value                                                    AS code
FROM ${openaire_db_name}.project p
WHERE p.datainfo.deletedbyinference = false;

create table ${stats_db_name}.funder as
select distinct xpath_string(fund, '//funder/id')        as id,
                xpath_string(fund, '//funder/name')      as name,
                xpath_string(fund, '//funder/shortname') as shortname
from ${openaire_db_name}.project p lateral view explode(p.fundingtree.value) fundingtree as fund;

ANALYZE TABLE ${stats_db_name}.project_oids COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.project_oids COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.project_organizations COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.project_organizations COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.project_results COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.project_results COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.project_tmp COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.project_tmp COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE ${stats_db_name}.funder COMPUTE STATISTICS;
ANALYZE TABLE ${stats_db_name}.funder COMPUTE STATISTICS FOR COLUMNS;