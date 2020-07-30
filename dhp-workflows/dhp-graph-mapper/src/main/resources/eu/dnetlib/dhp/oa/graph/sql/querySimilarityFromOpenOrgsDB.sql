SELECT 
	local_id                                              AS id1, 
	oa_original_id                                        AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance,
	'isSimilarTo'                                         AS reltype
FROM oa_duplicates WHERE reltype = 'is_similar' 

UNION ALL

SELECT 
	local_id                                              AS id1, 
	oa_original_id                                        AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance,
	'isDifferentFrom'                                     AS reltype
FROM oa_duplicates WHERE reltype = 'is_different'

UNION ALL

SELECT
	o.id                                                  AS id1,
	'openorgsmesh'||substring(o.id, 13)||'-'||md5(n.name) AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance,
	'is_similar'                                          AS reltype
FROM other_names n
	LEFT OUTER JOIN organizations o ON (n.id = o.id)
