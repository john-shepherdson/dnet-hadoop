INSERT INTO ${stats_db_name}.otherresearchproduct_tmp select substr(o.id, 4) as id, o.title[0].value as title, o.publisher.value as publisher, cast(null as string) as journal, 
o.dateofacceptance.value as date, date_format(o.dateofacceptance.value,'yyyy') as year, o.bestaccessright.classname as bestlicence,
o.embargoenddate.value as embargo_end_date, false as delayed, size(o.author) as authors , concat_ws('\u003B',o.source.value) as source,
case when size(o.description) > 0 then true else false end as abstract,
'other' as type 
from ${openaire_db_name}.otherresearchproduct o
where o.datainfo.deletedbyinference=false;
