select distinct 
	s.id as id
from 
	dsm_services s 
	join dsm_api a on (s.id = a.service) 
where 
	collectedfrom = 'openaire____::opendoar' 
	and (coalesce(a.compatibility_override, a.compatibility) like '%openaire%' or a.last_collection_total > 0);