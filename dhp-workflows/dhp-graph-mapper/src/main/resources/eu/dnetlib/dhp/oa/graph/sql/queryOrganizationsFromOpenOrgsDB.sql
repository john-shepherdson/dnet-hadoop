SELECT
	o.id                                                                    AS organizationid,
	coalesce((array_agg(a.acronym))[1], o.name)                             AS legalshortname,
	o.name                                                                  AS legalname,
	array_agg(DISTINCT n.name)                                              AS alternativenames,
	(array_agg(u.url))[1]                                                   AS websiteurl,
	''                                                                      AS logourl,
	DATE(o.creation_date)                                                   AS dateofcollection,
	DATE(o.modification_date)                                               AS dateoftransformation,
	false                                                                   AS ecenterprise,
	false                                                                   AS echighereducation,
	false                                                                   AS ecinternationalorganization,
	false                                                                   AS ecinternationalorganizationeurinterests,
	false                                                                   AS eclegalbody,
	false                                                                   AS eclegalperson,
	false                                                                   AS ecnonprofit,
	false                                                                   AS ecnutscode,
	false                                                                   AS ecresearchorganization,
	false                                                                   AS ecsmevalidated,
	false                                                                   AS inferred,
	false                                                                   AS deletedbyinference,
	0.99                                                                    AS trust,
	''                                                                      AS inferenceprovenance,
	'openaire____::openorgs'                                                AS collectedfromid,
	'OpenOrgs Database'                                                     AS collectedfromname,
	o.country || '@@@dnet:countries'                                        AS country,
	'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions'          AS provenanceaction,
	array_agg(DISTINCT i.otherid || '###' || i.type || '@@@dnet:pid_types') AS pid
FROM organizations o
	LEFT OUTER JOIN acronyms a    ON (a.id = o.id)
	LEFT OUTER JOIN urls u        ON (u.id = o.id)
	LEFT OUTER JOIN other_ids i   ON (i.id = o.id)
	LEFT OUTER JOIN other_names n ON (n.id = o.id)
GROUP BY
	o.id,
	o.name,
	o.modification_date,
	o.country
	
UNION ALL
	
SELECT
	'openorgsmesh'||substring(o.id, 13)||'-'||md5(n.name)                   AS organizationid,
	n.name                                                                  AS legalshortname,
	n.name                                                                  AS legalname,
	ARRAY[]::text[]                                                         AS alternativenames,
	(array_agg(u.url))[1]                                                   AS websiteurl,
	''                                                                      AS logourl,
	DATE(o.creation_date)                                                   AS dateofcollection,
	DATE(o.modification_date)                                               AS dateoftransformation,
	false                                                                   AS ecenterprise,
	false                                                                   AS echighereducation,
	false                                                                   AS ecinternationalorganization,
	false                                                                   AS ecinternationalorganizationeurinterests,
	false                                                                   AS eclegalbody,
	false                                                                   AS eclegalperson,
	false                                                                   AS ecnonprofit,
	false                                                                   AS ecnutscode,
	false                                                                   AS ecresearchorganization,
	false                                                                   AS ecsmevalidated,
	false                                                                   AS inferred,
	false                                                                   AS deletedbyinference,
	0.88                                                                    AS trust,
	''                                                                      AS inferenceprovenance,
	'openaire____::openorgs'                                                AS collectedfromid,
	'OpenOrgs Database'                                                     AS collectedfromname,
	o.country || '@@@dnet:countries'                                        AS country,
	'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions'          AS provenanceaction,
	array_agg(DISTINCT i.otherid || '###' || i.type || '@@@dnet:pid_types') AS pid
FROM other_names n
	LEFT OUTER JOIN organizations o ON (n.id = o.id)
	LEFT OUTER JOIN urls u          ON (u.id = o.id)
	LEFT OUTER JOIN other_ids i     ON (i.id = o.id)
GROUP BY
	o.id, o.modification_date, o.country, n.name


