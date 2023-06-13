SELECT
	o.id                                                                                                                          AS organizationid,
	coalesce((array_agg(a.acronym))[1], o.name)                                                                                   AS legalshortname,
	o.name                                                                                                                        AS legalname,
	array_remove(array_cat(array_agg(DISTINCT n.name), array_agg(DISTINCT a.acronym)), NULL)                                      AS alternativenames,
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
	ARRAY(SELECT DISTINCT pid FROM unnest(array_cat(
		array_agg(DISTINCT i.otherid    || '###' || i.type    || '@@@dnet:pid_types'), 
		array_agg(DISTINCT idup.otherid || '###' || idup.type || '@@@dnet:pid_types')
	)) as t(pid) where pid IS NOT NULL) AS pid,
    (array_remove(array_cat(ARRAY[o.ec_legalbody], array_agg(od.ec_legalbody)), NULL))[1]                      AS eclegalbody,
    (array_remove(array_cat(ARRAY[o.ec_legalperson], array_agg(od.ec_legalperson)), NULL))[1]                     AS eclegalperson,
    (array_remove(array_cat(ARRAY[o.ec_nonprofit], array_agg(od.ec_nonprofit)), NULL))[1]                      AS ecnonprofit,
    (array_remove(array_cat(ARRAY[o.ec_researchorganization], array_agg(od.ec_researchorganization)), NULL))[1]                 AS ecresearchorganization,
    (array_remove(array_cat(ARRAY[o.ec_highereducation], array_agg(od.ec_highereducation)), NULL))[1]                   AS echighereducation,
    (array_remove(array_cat(ARRAY[o.ec_internationalorganizationeurinterests], array_agg(od.ec_internationalorganizationeurinterests)), NULL))[1]        AS ecinternationalorganizationeurinterests,
    (array_remove(array_cat(ARRAY[o.ec_internationalorganization], array_agg(od.ec_internationalorganization)), NULL))[1]              AS ecinternationalorganization,
    (array_remove(array_cat(ARRAY[o.ec_enterprise], array_agg(od.ec_enterprise)), NULL))[1]                      AS ecenterprise,
    (array_remove(array_cat(ARRAY[o.ec_smevalidated], array_agg(od.ec_smevalidated)), NULL))[1]                    AS ecsmevalidated,
    (array_remove(array_cat(ARRAY[o.ec_nutscode], array_agg(od.ec_nutscode)), NULL))[1]                       AS ecnutscode
FROM organizations o
	LEFT OUTER JOIN acronyms a    ON (a.id = o.id)
	LEFT OUTER JOIN urls u        ON (u.id = o.id)
	LEFT OUTER JOIN other_ids i   ON (i.id = o.id)
	LEFT OUTER JOIN other_names n ON (n.id = o.id)
	LEFT OUTER JOIN oa_duplicates d ON (o.id = d.local_id AND d.reltype != 'is_different')
    LEFT OUTER JOIN organizations od ON (d.oa_original_id = od.id)
    LEFT OUTER JOIN other_ids idup  ON (od.id = idup.id)
WHERE
    o.status = 'approved' OR o.status = 'suggested'
GROUP BY
	o.id,
	o.name,
	o.creation_date,
	o.modification_date,
	o.country;
