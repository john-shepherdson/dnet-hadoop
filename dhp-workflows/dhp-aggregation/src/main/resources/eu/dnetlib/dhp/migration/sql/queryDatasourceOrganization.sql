SELECT
	dor.datasource                                                          AS datasource,
	dor.organization                                                        AS organization,
	NULL                                                                    AS startdate,
	NULL                                                                    AS enddate,
	false                                                                   AS inferred,
	false                                                                   AS deletedbyinference,
	0.9                                                                     AS trust,
	NULL                                                                    AS inferenceprovenance,
	dc.id                                                                   AS collectedfromid,
    dc.officialname                                                         AS collectedfromname,
	'providedBy@@@provided by@@@dnet:datasources_organizations_typologies@@@dnet:datasources_organizations_typologies' AS semantics,
	d.provenanceaction || '@@@' || d.provenanceaction || '@@@dnet:provenanceActions@@@dnet:provenanceActions' AS provenanceaction

FROM dsm_datasource_organization dor
	LEFT OUTER JOIN dsm_datasources d ON (dor.datasource = d.id)
	LEFT OUTER JOIN dsm_datasources dc ON (dc.id = d.collectedfrom)
