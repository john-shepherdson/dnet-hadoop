--------------------------------------------------------------
--------------------------------------------------------------
-- Publication table/view and Publication related tables/views
--------------------------------------------------------------
--------------------------------------------------------------

-- Publication temporary table
DROP TABLE IF EXISTS ${stats_db_name}.publication_tmp;

CREATE TABLE ${stats_db_name}.publication_tmp (id STRING,   title STRING,   publisher STRING,   journal STRING,   date STRING,   year STRING,   bestlicence STRING,   embargo_end_date STRING,   delayed BOOLEAN,   authors INT,   source STRING,   abstract BOOLEAN,   type STRING )  clustered by (id) into 100 buckets stored as orc tblproperties('transactional'='true');

INSERT INTO ${stats_db_name}.publication_tmp SELECT substr(p.id, 4) as id, p.title[0].value as title, p.publisher.value as publisher, p.journal.name as journal , 
p.dateofacceptance.value as date, date_format(p.dateofacceptance.value,'yyyy') as year, p.bestaccessright.classname as bestlicence,
p.embargoenddate.value as embargo_end_date, false as delayed, size(p.author) as authors , concat_ws('\u003B',p.source.value) as source,
case when size(p.description) > 0 then true else false end as abstract,
'publication' as type
from ${openaire_db_name}.publication p
where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.publication_classifications AS SELECT substr(p.id, 4) as id, instancetype.classname as type from ${openaire_db_name}.publication p LATERAL VIEW explode(p.instance.instancetype) instances as instancetype where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.publication_concepts AS SELECT substr(p.id, 4) as id, contexts.context.id as concept from ${openaire_db_name}.publication p LATERAL VIEW explode(p.context) contexts as context where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.publication_datasources as
SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource
  FROM (
	SELECT substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) as datasource
	from ${openaire_db_name}.publication p lateral view explode(p.instance) instances as instance
	where p.datainfo.deletedbyinference=false	) p
  LEFT OUTER JOIN (
	SELECT substr(d.id, 4) id
	from ${openaire_db_name}.datasource d
	WHERE d.datainfo.deletedbyinference=false ) d on p.datasource = d.id;

CREATE TABLE ${stats_db_name}.publication_languages AS select substr(p.id, 4) as id, p.language.classname as language FROM ${openaire_db_name}.publication p where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.publication_oids AS SELECT substr(p.id, 4) AS id, oids.ids AS oid FROM ${openaire_db_name}.publication p LATERAL VIEW explode(p.originalid) oids AS ids where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.publication_pids AS SELECT substr(p.id, 4) AS id, ppid.qualifier.classname AS type, ppid.value as pid FROM ${openaire_db_name}.publication p LATERAL VIEW explode(p.pid) pids AS ppid where p.datainfo.deletedbyinference=false;

CREATE TABLE ${stats_db_name}.publication_topics as select substr(p.id, 4) AS id, subjects.subject.qualifier.classname AS TYPE, subjects.subject.value AS topic FROM ${openaire_db_name}.publication p LATERAL VIEW explode(p.subject) subjects AS subject where p.datainfo.deletedbyinference=false;

-- Publication_citations
CREATE TABLE ${stats_db_name}.publication_citations AS SELECT substr(p.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS result FROM ${openaire_db_name}.publication p lateral view explode(p.extrainfo) citations AS citation WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="" and p.datainfo.deletedbyinference=false;