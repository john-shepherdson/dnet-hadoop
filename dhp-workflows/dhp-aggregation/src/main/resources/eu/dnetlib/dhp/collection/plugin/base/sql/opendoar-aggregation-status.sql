select 
	s.id           as id, 
	s.jurisdiction as jurisdiction, 
	array_remove(array_agg(a.id || ' (' || coalesce(a.compatibility_override, a.compatibility, 'UNKNOWN') || ')@@@' || coalesce(a.last_collection_total, 0)), NULL) as aggregations
from 
	dsm_services s 
	join dsm_api a on (s.id = a.service) 
where 
	collectedfrom = 'openaire____::opendoar'
group by 
	s.id;
