SELECT local_id AS id1, oa_original_id AS id2 FROM openaire_simrels WHERE reltype = 'is_similar'

UNION ALL

SELECT
	o.id                                                     AS id1,
	'openorgsmesh'||substring(o.id, 13)||'-'||md5(a.acronym) AS id2
FROM acronyms a
	LEFT OUTER JOIN organizations o ON (a.id = o.id)

UNION ALL
		
SELECT
	o.id                                                     AS id1,
	'openorgsmesh'||substring(o.id, 13)||'-'||md5(n.name)    AS id2
FROM other_names n
	LEFT OUTER JOIN organizations o ON (n.id = o.id)
