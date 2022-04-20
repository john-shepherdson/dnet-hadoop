SELECT
	d.id                                                                                                       AS datasourceid,
	d.id || array_agg(distinct di.pid)                                                                         AS identities,
	d.officialname                                                                                             AS officialname,
	d.englishname                                                                                              AS englishname,
	d.contactemail                                                                                             AS contactemail,
	CASE
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY ['openaire-cris_1.1'])
    			THEN
    				'openaire-cris_1.1@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY ['openaire4.0'])
                	THEN
                    		'openaire4.0@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY ['driver', 'openaire2.0'])
			THEN
				'driver-openaire2.0@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['driver'])
			THEN
				'driver@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire2.0'])
			THEN
				'openaire2.0@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire3.0'])
			THEN
				'openaire3.0@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire2.0_data'])
			THEN
				'openaire2.0_data@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['native'])
			THEN
				'native@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['hostedBy'])
			THEN
				'hostedBy@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['notCompatible'])
			THEN
			'notCompatible@@@dnet:datasourceCompatibilityLevel'
	ELSE
		'UNKNOWN@@@dnet:datasourceCompatibilityLevel'
	END                                                                                                        AS openairecompatibility,
	d.websiteurl                                                                                               AS websiteurl,
	d.logourl                                                                                                  AS logourl,
	array_agg(DISTINCT CASE WHEN a.protocol = 'oai' and last_aggregation_date is not null THEN a.baseurl ELSE NULL END)                              AS accessinfopackage,
	d.latitude                                                                                                 AS latitude,
	d.longitude                                                                                                AS longitude,
	d.namespaceprefix                                                                                          AS namespaceprefix,
	NULL                                                                                                       AS odnumberofitems,
	NULL                                                                                                       AS odnumberofitemsdate,

	(SELECT array_agg(s|| '###keywords@@@dnet:subject_classification_typologies')
		FROM UNNEST(
			ARRAY(
				SELECT trim(s)
        FROM unnest(string_to_array(d.subjects, '@@')) AS s)) AS s)                                   AS subjects,

	d.description                                                                                              AS description,
	NULL                                                                                                       AS odpolicies,
	ARRAY(SELECT trim(s)
	      FROM unnest(string_to_array(d.languages, ',')) AS s)                                                 AS odlanguages,
	
	-- Term provided only by OpenDOAR: 
	--   probably updating the TR it could be replaced by research_entity_types[]
	--   But a study on the vocabulary terms is needed
	--   REMOVED: ARRAY(SELECT trim(s) FROM unnest(string_to_array(d.od_contenttypes, '-')) AS s)                                           AS odcontenttypes,
	
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
	-- the following 2 fields (provided by re3data) have been replaced by research_entity_types[]
	--  VALUE 'Research Data' : d.dataprovider                                                                                             AS dataprovider,
	--  VALUE 'Services'      : d.serviceprovider                                                                                          AS serviceprovider,
	d.databaseaccesstype                                                                                       AS databaseaccesstype,
	d.datauploadtype                                                                                           AS datauploadtype,
	d.databaseaccessrestriction                                                                                AS databaseaccessrestriction,
	d.datauploadrestriction                                                                                    AS datauploadrestriction,
	-- REPLACED BY version_control : d.versioning                                                                                               AS versioning,
	d.version_control                                                                                               AS versioning,
	d.citationguidelineurl                                                                                     AS citationguidelineurl,
	-- REMOVED (it was provided only by re3data: yes, no, unknown): d.qualitymanagementkind                                                                                    AS qualitymanagementkind,
	d.pidsystems                                                                                               AS pidsystems,
	d.certificates                                                                                             AS certificates,
	ARRAY[]::text[]                                                                                            AS policies,
	dc.id                                                                                                      AS collectedfromid,
	dc.officialname                                                                                            AS collectedfromname,
	d._typology_to_remove_||'@@@dnet:datasource_typologies'                                                    AS datasourcetype,
	d.eosc_type||'@@@dnet:eosc_types'                                                                          AS eosc_type,
	d.eosc_datasource_type||'@@@dnet:eosc_datasource_types'                                                    AS eosc_datasoorce_type,
	'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions'                                             AS provenanceaction,
	d.issn                                                                                                     AS issnPrinted,
	d.eissn                                                                                                    AS issnOnline,
	d.lissn                                                                                                    AS issnLinking,
	d.consenttermsofuse                                                                                        AS consenttermsofuse,
	d.fulltextdownload                                                                                         AS fulltextdownload,
	d.consenttermsofusedate                                                                                    AS consenttermsofusedate,
	d.jurisdiction||'@@@eosc:jurisdictions'                                                                   AS jurisdiction,
	d.thematic                                                                                                AS thematic,
	-- REMOVED ???: d.knowledge_graph                                                                                         AS knowledgegraph,
	array(select unnest(d.content_policies)||'@@@eosc:contentpolicies')                                       AS contentpolicies

FROM dsm_services d
LEFT OUTER JOIN dsm_services dc on (d.collectedfrom = dc.id)
LEFT OUTER JOIN dsm_api a ON (d.id = a.service)
LEFT OUTER JOIN dsm_servicepids di ON (d.id = di.service)

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
	-- TODO REMOVED ???: d.dataprovider,
	-- TODO REMOVED ???: d.serviceprovider,
	d.databaseaccesstype,
	d.datauploadtype,
	d.databaseaccessrestriction,
	d.datauploadrestriction,
	-- REPLACED BY version_control : d.versioning,
	d.version_control
	d.citationguidelineurl,
	-- REMOVED: d.qualitymanagementkind,
	d.pidsystems,
	d.certificates,
	dc.id,
	dc.officialname,
	d.issn,
	d.eissn,
	d.lissn,
	d.jurisdiction,
	d.thematic,
	-- REMOVED ???: de.knowledge_graph,
	d.content_policies
