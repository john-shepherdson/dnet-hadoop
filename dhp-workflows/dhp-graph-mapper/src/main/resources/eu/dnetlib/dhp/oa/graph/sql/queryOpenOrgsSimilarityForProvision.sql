-- relations approved by the user and suggested by the dedup
SELECT
	local_id                                              AS id1,
	oa_original_id                                        AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance,
	'isSimilarTo'                                         AS relclass
FROM oa_duplicates WHERE reltype = 'is_similar' OR reltype = 'suggested';