SELECT
	po.project                                                              AS project,
	po.resporganization                                                     AS resporganization,
	po.participantnumber                                                    AS participantnumber,
	po.contribution                                                         AS contribution,
    po.currency                                                             AS currency,
	NULL                                                                    AS startdate,
	NULL                                                                    AS enddate,
	false                                                                   AS inferred,
	false                                                                   AS deletedbyinference,
	po.trust                                                                AS trust,
	NULL                                                                    AS inferenceprovenance,
	dc.id                                                                   AS collectedfromid,
    dc.officialname                                                         AS collectedfromname,
	po.semanticclass || '@@@dnet:project_organization_relations'            AS semantics,
	'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions'          AS provenanceaction

FROM project_organization po
	LEFT OUTER JOIN projects p ON (p.id = po.project)
	LEFT OUTER JOIN dsm_services dc ON (dc.id = p.collectedfrom);