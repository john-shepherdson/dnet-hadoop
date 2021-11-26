------------------------------------------------------
------------------------------------------------------
-- Additional relations
--
-- Refereed related tables/views
------------------------------------------------------
------------------------------------------------------

CREATE TABLE IF NOT EXISTS ${stats_db_name}.publication_refereed STORED AS PARQUET as
select substr(r.id, 4) as id, inst.refereed.classname as refereed
from ${openaire_db_name}.publication r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.dataset_refereed STORED AS PARQUET as
select substr(r.id, 4) as id, inst.refereed.classname as refereed
from ${openaire_db_name}.dataset r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.software_refereed STORED AS PARQUET as
select substr(r.id, 4) as id, inst.refereed.classname as refereed
from ${openaire_db_name}.software r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE;

CREATE TABLE IF NOT EXISTS ${stats_db_name}.otherresearchproduct_refereed STORED AS PARQUET as
select substr(r.id, 4) as id, inst.refereed.classname as refereed
from ${openaire_db_name}.otherresearchproduct r lateral view explode(r.instance) instances as inst
where r.datainfo.deletedbyinference=false and r.datainfo.invisible = FALSE;

CREATE VIEW IF NOT EXISTS ${stats_db_name}.result_refereed STORED AS PARQUET as
select * from ${stats_db_name}.publication_refereed
union all
select * from ${stats_db_name}.dataset_refereed
union all
select * from ${stats_db_name}.software_refereed
union all
select * from ${stats_db_name}.otherresearchproduct_refereed;