------------------------------------------------------
------------------------------------------------------
-- Additional relations
--
-- Refereed related tables/views
------------------------------------------------------
------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${stats_db_name}.publication_refereed as
select substr(r.id, 4) as id, inst.refereed.value as refereed
from ${openaire_db_name}.publication r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.dataset_refereed as
select substr(r.id, 4) as id, inst.refereed.value as refereed
from ${openaire_db_name}.dataset r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.software_refereed as
select substr(r.id, 4) as id, inst.refereed.value as refereed
from ${openaire_db_name}.software r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.otherresearchproduct_refereed as
select substr(r.id, 4) as id, inst.refereed.value as refereed
from ${openaire_db_name}.otherresearchproduct r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false;

CREATE VIEW IF NOT EXISTS ${stats_db_name}.result_refereed as
select * from ${stats_db_name}.publication_refereed
union all 
select * from ${stats_db_name}.dataset_refereed
union all
select * from ${stats_db_name}.software_refereed
union all
select * from ${stats_db_name}.otherresearchproduct_refereed;
