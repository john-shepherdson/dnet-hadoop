SELECT
		o.id                                                      AS organizationid,
		o.legalshortname                                          AS legalshortname,
		o.legalname                                               AS legalname,
		o.websiteurl                                              AS websiteurl,
		o.logourl                                                 AS logourl,
		o.ec_legalbody                                            AS eclegalbody,
		o.ec_legalperson                                          AS eclegalperson,
		o.ec_nonprofit                                            AS ecnonprofit,
		o.ec_researchorganization                                 AS ecresearchorganization,
		o.ec_highereducation                                      AS echighereducation,
		o.ec_internationalorganizationeurinterests                AS ecinternationalorganizationeurinterests,
		o.ec_internationalorganization                            AS ecinternationalorganization,
		o.ec_enterprise                                           AS ecenterprise,
		o.ec_smevalidated                                         AS ecsmevalidated,
		o.ec_nutscode                                             AS ecnutscode,
		o.dateofcollection                                        AS dateofcollection,
		o.lastupdate                                              AS dateoftransformation,
		false                                                     AS inferred,
		false                                                     AS deletedbyinference,
		o.trust                                                   AS trust,
		''                                                        AS inferenceprovenance,
		d.id                                                      AS collectedfromid,
		d.officialname                                            AS collectedfromname,

		o.country || '@@@dnet:countries'                          AS country,
		'sysimport:crosswalk:entityregistry@@@sysimport:crosswalk:entityregistry@@@dnet:provenance_actions@@@dnet:provenance_actions' AS provenanceaction,

		ARRAY[]::text[]                                           AS pid
FROM dsm_organizations o
	LEFT OUTER JOIN dsm_datasources d ON (d.id = o.collectedfrom)





