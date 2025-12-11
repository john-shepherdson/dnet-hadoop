SELECT
    r.org1                                               AS source,
    r.org2                                               AS target,
    'IsChildOf'                                          AS type,
    false                                                AS inferred,
    false                                                AS deletedbyinference,
    0.95                                                 AS trust,
    ''                                                   AS inferenceprovenance,
    s.id                                                 AS collectedfromid,
    s.officialname                                       AS collectedfromname,
    'sysimport:crosswalk:cris@@@dnet:provenance_actions' AS provenanceaction
FROM dsm_organization_rels r join dsm_organizations o join dsm_services s on (o.collectedfrom = s.id) on (r.org1 = o.id)
WHERE r.reltype = 'IsChildOf'

UNION ALL

SELECT
    r.org2                                               AS source,
    r.org1                                               AS target,
    'IsParentOf'                                         AS type,
    false                                                AS inferred,
    false                                                AS deletedbyinference,
    0.95                                                 AS trust,
    ''                                                   AS inferenceprovenance,
    s.id                                                 AS collectedfromid,
    s.officialname                                       AS collectedfromname,
    'sysimport:crosswalk:cris@@@dnet:provenance_actions' AS provenanceaction
FROM dsm_organization_rels r join dsm_organizations o join dsm_services s on (o.collectedfrom = s.id) on (r.org1 = o.id)
WHERE r.reltype = 'IsChildOf';