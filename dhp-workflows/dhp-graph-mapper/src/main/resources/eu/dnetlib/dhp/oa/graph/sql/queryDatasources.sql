SELECT
	d.id                                                                                                       AS datasourceid,
	d.id || array_agg(distinct di.pid)                                                                         AS identities,
	d.officialname                                                                                             AS officialname,
	d.englishname                                                                                              AS englishname,
	d.contactemail                                                                                             AS contactemail,      
	CASE
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY ['openaire-cris_1.1'])
    			THEN
    				'openaire-cris_1.1@@@OpenAIRE CRIS v1.1@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY ['driver', 'openaire2.0'])
			THEN
				'driver-openaire2.0@@@OpenAIRE 2.0+ (DRIVER OA, EC funding)@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['driver'])
			THEN
				'driver@@@OpenAIRE Basic (DRIVER OA)@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire2.0'])
			THEN
				'openaire2.0@@@OpenAIRE 2.0 (EC funding)@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire3.0'])
			THEN
				'openaire3.0@@@OpenAIRE 3.0 (OA, funding)@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire2.0_data'])
			THEN
				'openaire2.0_data@@@OpenAIRE Data (funded, referenced datasets)@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['native'])
			THEN
				'native@@@proprietary@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['hostedBy'])
			THEN
				'hostedBy@@@collected from a compatible aggregator@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['notCompatible'])
			THEN
			'notCompatible@@@under validation@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
	ELSE
		'UNKNOWN@@@not available@@@dnet:datasourceCompatibilityLevel@@@dnet:datasourceCompatibilityLevel'
	END                                                                                                        AS openairecompatibility,
	d.websiteurl                                                                                               AS websiteurl,
	d.logourl                                                                                                  AS logourl,
	array_agg(DISTINCT CASE WHEN a.protocol = 'oai' and last_aggregation_date is not null THEN a.baseurl ELSE NULL END)                              AS accessinfopackage,
	d.latitude                                                                                                 AS latitude,
	d.longitude                                                                                                AS longitude,
	d.namespaceprefix                                                                                          AS namespaceprefix,
	NULL                                                                                                       AS odnumberofitems,
	NULL                                                                                                       AS odnumberofitemsdate,

	(SELECT array_agg(s|| '###keywords@@@keywords@@@dnet:subject_classification_typologies@@@dnet:subject_classification_typologies')
		FROM UNNEST(
			ARRAY(
				SELECT trim(s)
        FROM unnest(string_to_array(d.subjects, '@@')) AS s)) AS s)                                   AS subjects,

	d.description                                                                                              AS description,
	NULL                                                                                                       AS odpolicies,
	ARRAY(SELECT trim(s)
	      FROM unnest(string_to_array(d.languages, ',')) AS s)                                                 AS odlanguages,
	ARRAY(SELECT trim(s)
	      FROM unnest(string_to_array(d.od_contenttypes, '-')) AS s)                                           AS odcontenttypes,
	false                                                                                                      AS inferred,
	false                                                                                                      AS deletedbyinference,
	0.9                                                                                                        AS trust,
	NULL                                                                                                       AS inferenceprovenance,
	d.dateofcollection                                                                                         AS dateofcollection,
	d.dateofvalidation                                                                                         AS dateofvalidation,
		-- re3data fields
	d.releasestartdate                                                                                         AS releasestartdate,
	d.releaseenddate                                                                                           AS releaseenddate,
	d.missionstatementurl                                                                                      AS missionstatementurl,
	d.dataprovider                                                                                             AS dataprovider,
	d.serviceprovider                                                                                          AS serviceprovider,
	d.databaseaccesstype                                                                                       AS databaseaccesstype,
	d.datauploadtype                                                                                           AS datauploadtype,
	d.databaseaccessrestriction                                                                                AS databaseaccessrestriction,
	d.datauploadrestriction                                                                                    AS datauploadrestriction,
	d.versioning                                                                                               AS versioning,
	d.citationguidelineurl                                                                                     AS citationguidelineurl,
	d.qualitymanagementkind                                                                                    AS qualitymanagementkind,
	d.pidsystems                                                                                               AS pidsystems,
	d.certificates                                                                                             AS certificates,
	ARRAY[]::text[]                                                                                            AS policies,
	dc.id                                                                                                      AS collectedfromid,
	dc.officialname                                                                                            AS collectedfromname,
	d.typology || '@@@' || CASE
		WHEN (d.typology = 'crissystem') THEN 'CRIS System'
		WHEN (d.typology = 'datarepository::unknown') THEN 'Data Repository'
		WHEN (d.typology = 'aggregator::datarepository') THEN 'Data Repository Aggregator'
		WHEN (d.typology = 'infospace') THEN 'Information Space'
		WHEN (d.typology = 'pubsrepository::institutional') THEN 'Institutional Repository'
		WHEN (d.typology = 'aggregator::pubsrepository::institutional') THEN 'Institutional Repository Aggregator'
		WHEN (d.typology = 'pubsrepository::journal') THEN 'Journal'
		WHEN (d.typology = 'aggregator::pubsrepository::journals') THEN 'Journal Aggregator/Publisher'
		WHEN (d.typology = 'pubsrepository::mock') THEN 'Other'
		WHEN (d.typology = 'pubscatalogue::unknown') THEN 'Publication Catalogue'
		WHEN (d.typology = 'pubsrepository::unknown') THEN 'Publication Repository'
		WHEN (d.typology = 'aggregator::pubsrepository::unknown') THEN 'Publication Repository Aggregator'
		WHEN (d.typology = 'entityregistry') THEN 'Registry'
		WHEN (d.typology = 'scholarcomminfra') THEN 'Scholarly Comm. Infrastructure'
		WHEN (d.typology = 'pubsrepository::thematic') THEN 'Thematic Repository'
		WHEN (d.typology = 'websource') THEN 'Web Source'
		WHEN (d.typology = 'entityregistry::projects') THEN 'Funder database'
		WHEN (d.typology = 'entityregistry::repositories') THEN 'Registry of repositories'
		WHEN (d.typology = 'softwarerepository') THEN 'Software Repository'
		WHEN (d.typology = 'aggregator::softwarerepository') THEN 'Software Repository Aggregator'
		WHEN (d.typology = 'orprepository') THEN 'Repository'
		ELSE 'Other'
	END || '@@@dnet:datasource_typologies@@@dnet:datasource_typologies'                               AS datasourcetype,
	'sysimport:crosswalk:entityregistry@@@sysimport:crosswalk:entityregistry@@@dnet:provenance_actions@@@dnet:provenance_actions' AS provenanceaction,
	CONCAT(d.issn, '@@@', d.eissn, '@@@', d.lissn)                                                    AS journal

FROM dsm_datasources d

LEFT OUTER JOIN dsm_datasources dc on (d.collectedfrom = dc.id)
LEFT OUTER JOIN dsm_api a ON (d.id = a.datasource)
LEFT OUTER JOIN dsm_datasourcepids di ON (d.id = di.datasource)

GROUP BY
	d.id,
	d.officialname,
	d.englishname,
	d.websiteurl,
	d.logourl,
	d.contactemail,
	d.namespaceprefix,
	d.description,
	d.latitude,
	d.longitude,
	d.dateofcollection,
	d.dateofvalidation,
	d.releasestartdate,
	d.releaseenddate,
	d.missionstatementurl,
	d.dataprovider,
	d.serviceprovider,
	d.databaseaccesstype,
	d.datauploadtype,
	d.databaseaccessrestriction,
	d.datauploadrestriction,
	d.versioning,
	d.citationguidelineurl,
	d.qualitymanagementkind,
	d.pidsystems,
	d.certificates,
	dc.id,
	dc.officialname,
	d.issn,
	d.eissn,
	d.lissn
