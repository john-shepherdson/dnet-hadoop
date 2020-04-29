INSERT INTO ${stats_db_name}.dataset_tmp select substr(d.id, 4) as id, d.title[0].value as title, d.publisher.value as publisher, cast(null as string) as journal, 
d.dateofacceptance.value as date, date_format(d.dateofacceptance.value,'yyyy') as year, d.bestaccessright.classname as bestlicence,
d.embargoenddate.value as embargo_end_date, false as delayed, size(d.author) as authors , concat_ws('\u003B',d.source.value) as source,
 case when size(d.description) > 0 then true else false end as abstract,
'dataset' as type
from ${openaire_db_name}.dataset d
where d.datainfo.deletedbyinference=false;
