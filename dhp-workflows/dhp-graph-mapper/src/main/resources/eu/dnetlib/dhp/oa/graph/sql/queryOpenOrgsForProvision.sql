SELECT
	o.id                                                                                                                          AS organizationid,
	coalesce((array_agg(a.acronym))[1], o.name)                                                                                   AS legalshortname,
	o.name                                                                                                                        AS legalname,
	array_agg(DISTINCT n.name)                                                                                                    AS "alternativeNames",
	(array_agg(u.url))[1]                                                                                                         AS websiteurl,
	''                                                                                                                            AS logourl,
	o.creation_date                                                                                                               AS dateofcollection,
	o.modification_date                                                                                                           AS dateoftransformation,
	false                                                                                                                         AS inferred,
	false                                                                                                                         AS deletedbyinference,
	0.95                                                                                                                          AS trust,
	''                                                                                                                            AS inferenceprovenance,
	'openaire____::openorgs'                                                                                                      AS collectedfromid,
	'OpenOrgs Database'                                                                                                           AS collectedfromname,
	o.country || '@@@dnet:countries'                                                       AS country,
	'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions' AS provenanceaction,
	array_agg(DISTINCT i.otherid || '###' || i.type || '@@@dnet:pid_types')                                                       AS pid,
			null                                            AS eclegalbody,
    		null                                          AS eclegalperson,
    		null                                            AS ecnonprofit,
    		null                                 AS ecresearchorganization,
    		null                                     AS echighereducation,
    		null                AS ecinternationalorganizationeurinterests,
    		null                           AS ecinternationalorganization,
    		null                                           AS ecenterprise,
    		null                                        AS ecsmevalidated,
    		null                                             AS ecnutscode
FROM organizations o
	LEFT OUTER JOIN acronyms a    ON (a.id = o.id)
	LEFT OUTER JOIN urls u        ON (u.id = o.id)
	LEFT OUTER JOIN other_ids i   ON (i.id = o.id)
	LEFT OUTER JOIN other_names n ON (n.id = o.id)
WHERE
    o.status = 'approved'
GROUP BY
	o.id,
	o.name,
	o.creation_date,
	o.modification_date,
	o.country;