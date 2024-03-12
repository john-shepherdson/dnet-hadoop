select s.id as id 
from dsm_services s 
where collectedfrom = 'openaire____::opendoar' 
and jurisdiction = 'Institutional'
and s.id not in (
	select service from dsm_api where coalesce(compatibility_override, compatibility) like '%openaire%' or last_collection_total > 0
);