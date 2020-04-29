CREATE TABLE ${stats_db_name}.software_datasources as SELECT p.id, case when d.id is null then 'other' else p.datasource end as datasource FROM (SELECT  substr(p.id, 4) as id, substr(instances.instance.hostedby.key, 4) as datasource 
from ${openaire_db_name}.software p lateral view explode(p.instance) instances as instance) p LEFT OUTER JOIN
(SELECT substr(d.id, 4) id from ${openaire_db_name}.datasource d WHERE d.datainfo.deletedbyinference=false) d on p.datasource = d.id;
