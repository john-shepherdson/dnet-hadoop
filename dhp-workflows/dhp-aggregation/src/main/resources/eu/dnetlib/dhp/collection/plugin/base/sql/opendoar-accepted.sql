select s.id as id 
from dsm_services s 
where collectedfrom = 'openaire____::opendoar' 
and jurisdiction = 'Institutional'
and s.id in (
	select service from dsm_api where coalesce(compatibility_override, compatibility) = 'driver' or coalesce(compatibility_override, compatibility) = 'UNKNOWN'
) and s.id not in (
	select service from dsm_api where coalesce(compatibility_override, compatibility) like '%openaire%'
);
