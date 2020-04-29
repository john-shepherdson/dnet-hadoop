-- The following throws the following exception on CRN HUE Hive:
-- Error while compiling statement: FAILED: SemanticException [Error 10011]: Line 2:34 Invalid function 'date_format'
-- But runs OK on OCEAN HUE Hive

INSERT INTO ${stats_db_name}.publication_tmp SELECT substr(p.id, 4) as id, p.title[0].value as title, p.publisher.value as publisher, p.journal.name as journal , 
p.dateofacceptance.value as date, date_format(p.dateofacceptance.value,'yyyy') as year, p.bestaccessright.classname as bestlicence,
p.embargoenddate.value as embargo_end_date, false as delayed, size(p.author) as authors , concat_ws('\u003B',p.source.value) as source,
case when size(p.description) > 0 then true else false end as abstract,
'publication' as type
from ${openaire_db_name}.publication p
where p.datainfo.deletedbyinference=false;

-- INSERT INTO ${hive_db_name}.publication_tmp SELECT substr(p.id, 4) as id, p.title[0].value as title, p.publisher.value as publisher, p.journal.name as journal,
-- p.dateofacceptance.value as date, date_format(p.dateofacceptance.value,'yyyy') as year, p.bestaccessright.classname as bestlicence,
-- p.embargoenddate.value as embargo_end_date, false as delayed, size(p.author) as authors , concat_ws('\u003B',p.source.value) as source,
-- case when size(p.description) > 0 then true else false end as abstract,
-- 'publication' as type
-- from openaire.publication p
-- where p.datainfo.deletedbyinference=false;
