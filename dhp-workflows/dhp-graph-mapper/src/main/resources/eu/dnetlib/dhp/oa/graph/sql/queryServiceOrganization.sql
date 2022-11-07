SELECT
	dor.service                                              AS service,
	dor.organization                                         AS organization,
	NULL                                                     AS startdate,
	NULL                                                     AS enddate,
	false                                                    AS inferred,
	false                                                    AS deletedbyinference,
	0.9                                                      AS trust,
	NULL                                                     AS inferenceprovenance,
	dc.id                                                    AS collectedfromid,
    dc.officialname                                          AS collectedfromname,
	'providedBy@@@dnet:datasources_organizations_typologies' AS semantics,
	d.provenanceaction || '@@@dnet:provenanceActions'        AS provenanceaction
FROM dsm_service_organization dor
	LEFT OUTER JOIN dsm_services d  ON (dor.service = d.id)
	LEFT OUTER JOIN dsm_services dc ON (dc.id = d.collectedfrom);