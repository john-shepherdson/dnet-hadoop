SELECT
	d.id                                                                                                       AS id,
	array_remove(d.id || array_agg(distinct CASE WHEN dp.pid like 'piwik%' THEN di.pid ELSE NULL END) || array_agg(distinct dds.duplicate), NULL)                AS originalid,
	array_remove(array_agg(distinct CASE WHEN di.pid NOT LIKE 'piwik%' THEN di.pid||'###'||di.issuertype||'@@@'||'dnet:pid_types' ELSE NULL END), NULL) as pid,
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
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire3.0'])
			THEN
				'openaire3.0@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire2.0_data'])
			THEN
				'openaire2.0_data@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility):: TEXT) @> ARRAY ['driver', 'openaire2.0'])
			THEN
				'driver-openaire2.0@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['openaire2.0'])
			THEN
				'openaire2.0@@@dnet:datasourceCompatibilityLevel'
		WHEN (array_agg(DISTINCT COALESCE (a.compatibility_override, a.compatibility) :: TEXT) @> ARRAY ['driver'])
			THEN
				'driver@@@dnet:datasourceCompatibilityLevel'
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
	END 	                                                                                                      AS openairecompatibility,
	d.websiteurl                                                                                               AS websiteurl,
	d.logourl                                                                                                  AS logourl,
	array_remove(array_agg(DISTINCT CASE WHEN a.protocol = 'oai' and last_aggregation_date is not null THEN a.baseurl ELSE NULL END), NULL)  AS accessinfopackage,
	d.latitude                                                                                                 AS latitude,
	d.longitude                                                                                                AS longitude,
	d.namespaceprefix                                                                                          AS namespaceprefix,
	NULL                                                                                                       AS odnumberofitems,
	NULL                                                                                                       AS odnumberofitemsdate,
	(SELECT array_agg(s|| '###keyword@@@dnet:subject_classification_typologies')
		FROM UNNEST(
			ARRAY(
				SELECT trim(s)
        FROM unnest(string_to_array(d.subjects, '@@')) AS s)) AS s)                                            AS subjects,

	d.description                                                                                              AS description,
	NULL                                                                                                       AS odpolicies,
	array_remove(ARRAY(SELECT trim(s)
	      FROM unnest(string_to_array(regexp_replace(d.languages, '{|}|"', '', 'g'), ',')) AS s), '{}')         AS odlanguages,
	array_remove(ARRAY(SELECT trim(s)
	      FROM unnest(string_to_array(regexp_replace(d.languages, '{|}|"', '', 'g'), ',')) AS s), '{}')         AS languages,
	-- Term provided only by     OpenDOAR:
	--   probably updating the TR it could be replaced by research_entity_types[]
	--   But a study on the vocabulary terms is needed
	--   REMOVED: ARRAY(SELECT trim(s) FROM unnest(string_to_array(d.od_contenttypes, '-')) AS s)              AS odcontenttypes,
	
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
	--  VALUE 'Research Data' : d.dataprovider                                                                 AS dataprovider,
	--  VALUE 'Services'      : d.serviceprovider                                                              AS serviceprovider,
	d.databaseaccesstype                                                                                       AS databaseaccesstype,
	d.datauploadtype                                                                                           AS datauploadtype,
	d.databaseaccessrestriction                                                                                AS databaseaccessrestriction,
	d.datauploadrestriction                                                                                    AS datauploadrestriction,
	-- REPLACED BY version_control : d.versioning                                                              AS versioning,
	d.version_control                                                                                          AS versioning,
	d.version_control                                                                                          AS versioncontrol,
	d.citationguidelineurl                                                                                     AS citationguidelineurl,
	array_to_string(array_agg(distinct dps.scheme), ' ')                                                       AS pidsystems,
	d.certificates                                                                                             AS certificates,
	ARRAY[]::text[]                                                                                            AS policies,
	array_remove(
	    array(
        select distinct cf
        from unnest(
            dc.id||'@@@'||dc.officialname || array_agg(distinct dds_cf.id||'@@@'||dds_cf.officialname)
        ) as cf),
	    NULL)                                                                                                  AS collectedfrom,
	d._typology_to_remove_||'@@@dnet:datasource_typologies'                                                    AS datasourcetype,
	d._typology_to_remove_||'@@@dnet:datasource_typologies_ui'                                                 AS datasourcetypeui,
	d.eosc_type||'@@@dnet:eosc_types'                                                                          AS eosctype,
	d.eosc_datasource_type||'@@@dnet:eosc_datasource_types'                                                    AS eoscdatasourcetype,
	d.issn                                                                                                     AS issnPrinted,
	d.eissn                                                                                                    AS issnOnline,
	d.lissn                                                                                                    AS issnLinking,
	d.research_entity_types                                                                                    AS researchentitytypes,
	d.consenttermsofuse                                                                                        AS consenttermsofuse,
	d.fulltextdownload                                                                                         AS fulltextdownload,
	d.consenttermsofusedate                                                                                    AS consenttermsofusedate,
	d.lastconsenttermsofusedate                                                                                AS lastconsenttermsofusedate,
	d.jurisdiction||'@@@eosc:jurisdictions'                                                                    AS jurisdiction,
	d.thematic                                                                                                 AS thematic,
	array(select unnest(d.content_policies)||'@@@eosc:contentpolicies')                                        AS contentpolicies,
	nullif(trim(d.submission_policy_url), '')                                                                  AS submissionpolicyurl,
	nullif(trim(d.preservation_policy_url), '')                                                                                  AS preservationpolicyurl,
	array_remove(d.research_product_access_policies, '')                                                       AS researchproductaccesspolicies,
    array_remove(d.research_product_metadata_access_policies, '')                                              AS researchproductmetadataaccesspolicies

FROM dsm_services d
LEFT OUTER JOIN dsm_services dc on (d.collectedfrom = dc.id)
LEFT OUTER JOIN dsm_api a ON (d.id = a.service)
LEFT OUTER JOIN dsm_servicepids dp ON (d.id = dp.service)
LEFT OUTER JOIN dsm_identities di ON (dp.pid = di.pid)
LEFT OUTER JOIN dsm_dedup_services dds ON (d.id = dds.id)
LEFT OUTER JOIN dsm_services dds_dup ON (dds.duplicate = dds_dup.id)
LEFT OUTER JOIN dsm_services dds_cf ON (dds_dup.collectedfrom = dds_cf.id)
LEFT OUTER JOIN dsm_pid_systems dps ON (d.id = dps.service)

WHERE
    d.dedup_main_service = true

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
	d.version_control,
	d.citationguidelineurl,
	-- REMOVED: d.qualitymanagementkind,
	d.certificates,
	dc.id,
	dc.officialname,
	d.issn,
	d.eissn,
	d.lissn,
	d.jurisdiction,
	d.thematic,
	-- REMOVED ???: de.knowledge_graph,
	d.content_policies,
	d.submission_policy_url,
	d.preservation_policy_url,
	d.research_product_access_policies,
    d.research_product_metadata_access_policies