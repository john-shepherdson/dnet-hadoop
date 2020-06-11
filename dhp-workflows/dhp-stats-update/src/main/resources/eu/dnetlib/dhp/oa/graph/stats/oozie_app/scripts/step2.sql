--------------------------------------------------------------
--------------------------------------------------------------
-- 2. Publication table/view and Publication related tables/views
--------------------------------------------------------------
--------------------------------------------------------------

-- Publication temporary table
DROP TABLE IF EXISTS ${hive_db_name}.publication_tmp;
CREATE TABLE ${hive_db_name}.publication_tmp (id STRING,   title STRING,   publisher STRING,   journal STRING,   date STRING,   year STRING,   bestlicence STRING,   embargo_end_date STRING,   delayed BOOLEAN,   authors INT,   source STRING,   abstract BOOLEAN,   type STRING )  clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');

-- The following fails
--
INSERT INTO ${hive_db_name}.publication_tmp SELECT substr(p.id, 4) as id, p.title[0].value as title, p.publisher.value as publisher, p.journal.name as journal,
p.dateofacceptance.value as date, date_format(p.dateofacceptance.value,'yyyy') as year, p.bestaccessright.classname as bestlicence,
p.embargoenddate.value as embargo_end_date, false as delayed, size(p.author) as authors , concat_ws('\u003B',p.source.value) as source,
case when size(p.description) > 0 then true else false end as abstract,
'publication' as type
from ${hive_source_db_name}.publication p
where p.datainfo.deletedbyinference=false;


CREATE TABLE ${hive_db_name}.publication_classifications AS SELECT substr(p.id, 4) as id, instancetype.classname as type from ${hive_source_db_name}.publication p LATERAL VIEW explode(p.instance.instancetype) instances as instancetype;
CREATE TABLE ${hive_db_name}.publication_concepts AS SELECT substr(p.id, 4) as id, contexts.context.id as concept from ${hive_source_db_name}.publication p LATERAL VIEW explode(p.context) contexts as context;

CREATE TABLE ${hive_db_name}.publication_datasources as SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource FROM (SELECT  substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) as datasource
from ${hive_source_db_name}.publication p lateral view explode(p.instance) instances as instance) p LEFT OUTER JOIN
(SELECT substr(d.id, 4) id from ${hive_source_db_name}.datasource d WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;

CREATE TABLE ${hive_db_name}.publication_languages AS select substr(p.id, 4) as id, p.language.classname as language from ${hive_source_db_name}.publication p;
CREATE TABLE ${hive_db_name}.publication_oids AS SELECT substr(p.id, 4) as id, oids.ids as oid from ${hive_source_db_name}.publication p LATERAL VIEW explode(p.originalid) oids as ids;
create table ${hive_db_name}.publication_pids as select substr(p.id, 4) as id, ppid.qualifier.classname as type, ppid.value as pid from ${hive_source_db_name}.publication p lateral view explode(p.pid) pids as ppid;
create table ${hive_db_name}.publication_topics as select substr(p.id, 4) as id, subjects.subject.qualifier.classname as type, subjects.subject.value as topic from ${hive_source_db_name}.publication p lateral view explode(p.subject) subjects as subject;
