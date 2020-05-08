insert into ${stats_db_name}.datasource_tmp SELECT substr(d.id, 4) as id, officialname.value as name, datasourcetype.classname as type, dateofvalidation.value as dateofvalidation, date_format(d.dateofvalidation.value,'yyyy') as yearofvalidation, false as harvested, 0 as piwik_id, d.latitude.value as latitude, d.longitude.value as longitude, d.websiteurl.value as websiteurl, d.openairecompatibility.classid as compatibility
from ${openaire_db_name}.datasource d
WHERE d.datainfo.deletedbyinference=false;
