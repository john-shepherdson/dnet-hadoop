--------------------------------------------------------
--------------------------------------------------------
-- 5. Software table/view and Software related tables/views
--------------------------------------------------------
--------------------------------------------------------

-- Software temporary table supporting updates
DROP TABLE IF EXISTS ${hive_db_name}.software_tmp;
CREATE TABLE ${hive_db_name}.software_tmp (   id STRING,   title STRING,   publisher STRING,   journal STRING,   date STRING,   year STRING,   bestlicence STRING,   embargo_end_date STRING,   delayed BOOLEAN,   authors INT,   source STRING,   abstract BOOLEAN,   type STRING )  clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');
INSERT INTO ${hive_db_name}.software_tmp select substr(s.id, 4) as id, s.title[0].value as title, s.publisher.value as publisher, cast(null as string) as journal,
s.dateofacceptance.value as date, date_format(s.dateofacceptance.value,'yyyy') as year, s.bestaccessright.classname as bestlicence,
s.embargoenddate.value as embargo_end_date, false as delayed, size(s.author) as authors , concat_ws('\u003B',s.source.value) as source,
 case when size(s.description) > 0 then true else false end as abstract,
'software' as type
from ${hive_source_db_name}.software s
where s.datainfo.deletedbyinference=false;


-- Software_citations
Create table ${hive_db_name}.software_citations as select substr(s.id, 4) as id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") as result from ${hive_source_db_name}.software s  lateral view explode(s.extrainfo) citations as citation where xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="";

CREATE TABLE ${hive_db_name}.software_classifications AS SELECT substr(p.id, 4) as id, instancetype.classname as type from ${hive_source_db_name}.software p LATERAL VIEW explode(p.instance.instancetype) instances as instancetype;
CREATE TABLE ${hive_db_name}.software_concepts AS SELECT substr(p.id, 4) as id, contexts.context.id as concept from ${hive_source_db_name}.software p LATERAL VIEW explode(p.context) contexts as context;

CREATE TABLE ${hive_db_name}.software_datasources as SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource FROM (SELECT  substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) as datasource
from ${hive_source_db_name}.software p lateral view explode(p.instance) instances as instance) p LEFT OUTER JOIN
(SELECT substr(d.id, 4) id from ${hive_source_db_name}.datasource d WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;

CREATE TABLE ${hive_db_name}.software_languages AS select substr(p.id, 4) as id, p.language.classname as language from ${hive_source_db_name}.software p;
CREATE TABLE ${hive_db_name}.software_oids AS SELECT substr(p.id, 4) as id, oids.ids as oid from ${hive_source_db_name}.software p LATERAL VIEW explode(p.originalid) oids as ids;
create table ${hive_db_name}.software_pids as select substr(p.id, 4) as id, ppid.qualifier.classname as type, ppid.value as pid from ${hive_source_db_name}.software p lateral view explode(p.pid) pids as ppid;
create table ${hive_db_name}.software_topics as select substr(p.id, 4) as id, subjects.subject.qualifier.classname as type, subjects.subject.value as topic from ${hive_source_db_name}.software p lateral view explode(p.subject) subjects as subject;
