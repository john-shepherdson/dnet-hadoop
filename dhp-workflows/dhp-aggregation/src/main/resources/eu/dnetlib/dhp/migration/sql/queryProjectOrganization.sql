SELECT
	po.project                                                              AS project,
	po.resporganization                                                     AS resporganization,
	po.participantnumber                                                    AS participantnumber,
	po.contribution                                                         AS contribution,
	NULL                                                                    AS startdate,
	NULL                                                                    AS enddate,
	false                                                                   AS inferred,
	false                                                                   AS deletedbyinference,
	po.trust                                                                AS trust,
	NULL                                                                    AS inferenceprovenance,

	po.semanticclass || '@@@' || po.semanticclass || '@@@dnet:project_organization_relations@@@dnet:project_organization_relations' AS semantics,
	'sysimport:crosswalk:entityregistry@@@sysimport:crosswalk:entityregistry@@@dnet:provenance_actions@@@dnet:provenance_actions' AS provenanceaction

FROM project_organization po
