-- relations approved by the user and suggested by the dedup
SELECT
	d.local_id                                              AS id1,
	d.oa_original_id                                        AS id2,
	'openaire____::openorgs'                              AS collectedfromid,
	'OpenOrgs Database'                                   AS collectedfromname,
	false                                                 AS inferred,
	false                                                 AS deletedbyinference,
	0.99                                                  AS trust,
	''                                                    AS inferenceprovenance
FROM
    oa_duplicates d

LEFT OUTER JOIN
    organizations o ON (d.local_id = o.id)

WHERE
    (d.reltype = 'is_similar' OR d.reltype = 'suggested') AND (o.status != 'hidden');