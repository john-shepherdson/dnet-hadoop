-- relations approved by the user
SELECT
	d.local_id                                              AS id1,
	d.oa_original_id                                        AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance,
	'isSimilarTo'                                         AS relclass
FROM oa_duplicates d
LEFT OUTER JOIN organizations o ON (d.local_id=o.id)
WHERE d.reltype = 'is_similar' AND o.status = 'approved'

UNION ALL

-- relations between openorgs and mesh (alternative names)
SELECT
	o.id                                                  AS id1,
	'openorgsmesh'||substring(o.id, 13)||'-'||md5(n.name) AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance,
	'isSimilarTo'                                         AS relclass
FROM other_names n
LEFT OUTER JOIN organizations o ON (n.id = o.id)
WHERE o.status = 'approved'

UNION ALL

-- diff relations approved by the user
SELECT
	d.local_id                                              AS id1,
	d.oa_original_id                                        AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance,
	'isDifferentFrom'                                     AS relclass
FROM oa_duplicates d
LEFT OUTER JOIN organizations o ON (d.local_id=o.id)
WHERE d.reltype = 'is_different' AND o.status = 'approved';