-- datasource sources:
-- where the datasource info have been collected from.
create table if not exists ${stats_db_name}.datasource_sources AS
select substr(d.id,4) as id, substr(cf.key, 4) as datasource
from ${openaire_db_name}.datasource d lateral view explode(d.collectedfrom) cfrom as cf
where d.datainfo.deletedbyinference=false;