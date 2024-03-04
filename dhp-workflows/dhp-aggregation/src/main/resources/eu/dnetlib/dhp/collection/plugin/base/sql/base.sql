BEGIN;

INSERT INTO dsm_services(
	_dnet_resource_identifier_, 
	id,
	officialname,
	englishname,
	namespaceprefix,
	websiteurl,
	logourl,
	platform,
	contactemail,
	collectedfrom,
	provenanceaction,
	_typology_to_remove_,
	eosc_type,
	eosc_datasource_type,
	research_entity_types,
	thematic
) VALUES (
	'openaire____::base_search',
	'openaire____::base_search',
	'Bielefeld Academic Search Engine (BASE)',
	'Bielefeld Academic Search Engine (BASE)',
	'base_search_',
	'https://www.base-search.net',
	'https://www.base-search.net/about/download/logo_224x57_white.gif',
	'BASE',
	'',
	'infrastruct_::openaire',
	'user:insert',
	'aggregator::pubsrepository::unknown',
	'Data Source',
	'Aggregator',
	ARRAY['Research Products'],
	false
);

INSERT INTO dsm_service_organization(
	_dnet_resource_identifier_,
	organization,
	service
) VALUES (
	'openaire____::base_search::Bielefeld University Library@@openaire____::base_search',
	'openaire____::base_search::Bielefeld University Library',
	'openaire____::base_search'
);

INSERT INTO dsm_api(
	_dnet_resource_identifier_,
	id,
	service,
	protocol,
	baseurl,
	metadata_identifier_path
) VALUES (
	'api_________::openaire____::base_search::dump',
	'api_________::openaire____::base_search::dump',
	'openaire____::base_search',
	'baseDump',
	'/user/michele.artini/base-import/base_oaipmh_dump-current.tar',
	'//*[local-name()=''header'']/*[local-name()=''identifier'']'
);


INSERT INTO dsm_apiparams(
	_dnet_resource_identifier_, 
	api, 
	param, 
	value
) VALUES (
	'api_________::openaire____::base_search::dump@@dbUrl',
	'api_________::openaire____::base_search::dump',
	'dbUrl',
	'jdbc:postgresql://postgresql.services.openaire.eu:5432/dnet_openaireplus'
);

INSERT INTO dsm_apiparams(
	_dnet_resource_identifier_, 
	api, 
	param, 
	value
) VALUES (
	'api_________::openaire____::base_search::dump@@dbUser',
	'api_________::openaire____::base_search::dump',
	'dbUser',
	'dnet'
);

INSERT INTO dsm_apiparams(
	_dnet_resource_identifier_, 
	api, 
	param, 
	value
) VALUES (
	'api_________::openaire____::base_search::dump@@dbPassword',
	'api_________::openaire____::base_search::dump',
	'dbPassword',
	'***'
);

COMMIT;