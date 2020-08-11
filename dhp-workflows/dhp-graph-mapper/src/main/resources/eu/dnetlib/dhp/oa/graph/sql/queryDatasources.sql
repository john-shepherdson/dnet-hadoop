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
	d.typology||'@@@dnet:datasource_typologies'                                                                AS datasourcetype,
	'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions' AS provenanceaction,
	d.issn || ' @@@ ' || d.eissn || ' @@@ ' || d.lissn                                                       AS journal

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
