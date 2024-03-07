SELECT
	id1                                                            AS source,
	id2                                                            AS target,
	reltype                                                        AS type,
	false                                                          AS inferred,
	false                                                          AS deletedbyinference,
	0.95                                                           AS trust,
	''                                                             AS inferenceprovenance,
	'openaire____::openorgs'                                       AS collectedfromid,
	'OpenOrgs Database'                                            AS collectedfromname,
	'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions' AS provenanceaction
FROM relationships
WHERE reltype = 'IsChildOf' OR reltype = 'IsParentOf'