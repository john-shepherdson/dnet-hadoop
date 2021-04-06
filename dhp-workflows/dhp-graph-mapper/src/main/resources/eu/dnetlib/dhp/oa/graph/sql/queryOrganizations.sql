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
		'sysimport:crosswalk:entityregistry@@@dnet:provenance_actions' AS provenanceaction,
		 array_agg(DISTINCT i.pid || '###' || i.issuertype || '@@@dnet:pid_types') AS pid
FROM dsm_organizations o
	LEFT OUTER JOIN dsm_datasources d ON (d.id = o.collectedfrom)
	LEFT OUTER JOIN dsm_organizationpids p ON (p.organization = o.id)
	LEFT OUTER JOIN dsm_identities i ON (i.pid = p.pid)
GROUP BY
	o.id,
	o.legalshortname,
	o.legalname,
	o.websiteurl,
	o.logourl,
	o.ec_legalbody,
	o.ec_legalperson,
	o.ec_nonprofit,
	o.ec_researchorganization,
	o.ec_highereducation,
	o.ec_internationalorganizationeurinterests,
	o.ec_internationalorganization,
	o.ec_enterprise,
	o.ec_smevalidated,
	o.ec_nutscode,
	o.dateofcollection,
	o.lastupdate,
	o.trust,
	d.id,
	d.officialname,
	o.country;

-- TODO modificare in modo da fare il merge dei pid di tutti i record mergiati (per gli openorgs, approvati)
-- TODO invece per tutti gli altri con dei duplicati non fare questa cosa