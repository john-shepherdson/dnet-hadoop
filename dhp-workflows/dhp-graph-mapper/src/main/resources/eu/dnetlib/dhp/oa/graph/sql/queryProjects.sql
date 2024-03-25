SELECT
                p.id                                                                                                       AS projectid,
                p.code                                                                                                     AS code,
                p.websiteurl                                                                                               AS websiteurl,
                p.acronym                                                                                                  AS acronym,
                p.title                                                                                                    AS title,
                p.startdate                                                                                                AS startdate,
                p.enddate                                                                                                  AS enddate,
                p.call_identifier                                                                                          AS callidentifier,
                p.keywords                                                                                                 AS keywords,
                p.duration                                                                                                 AS duration,
                p.ec_sc39                                                                                                  AS ecsc39,
                p.oa_mandate_for_publications                                                                              AS oamandatepublications,
                p.ec_article29_3                                                                                           AS ecarticle29_3,
                p.dateofcollection                                                                                         AS dateofcollection,
                p.lastupdate                                                                                               AS dateoftransformation,
                p.inferred                                                                                                 AS inferred,
                p.deletedbyinference                                                                                       AS deletedbyinference,
                p.trust                                                                                                    AS trust,
                p.inferenceprovenance                                                                                      AS inferenceprovenance,
                p.optional1                                                                                                AS optional1,
                p.optional2                                                                                                AS optional2,
                p.jsonextrainfo                                                                                            AS jsonextrainfo,
                p.contactfullname                                                                                          AS contactfullname,
                p.contactfax                                                                                               AS contactfax,
                p.contactphone                                                                                             AS contactphone,
                p.contactemail                                                                                             AS contactemail,
                p.summary                                                                                                  AS summary,
                p.currency                                                                                                 AS currency,
                p.totalcost                                                                                                AS totalcost,
                p.fundedamount                                                                                             AS fundedamount,
                dc.id                                                                                                      AS collectedfromid,
                dc.officialname                                                                                            AS collectedfromname,
                p.contracttype || '@@@' || p.contracttypescheme                                                            AS contracttype,
                p.provenanceactionclass || '@@@' || p.provenanceactionscheme                                             AS provenanceaction,
                array_remove(array_agg(DISTINCT i.pid || '###' || i.issuertype || '@@@' || i.issuertype), NULL)            AS pid,,
                array_agg(DISTINCT s.name || '###' || s.semanticclass || '@@@' || s.semanticscheme)          AS subjects,
                array_agg(DISTINCT fp.path)                                                                                         AS fundingtree

        FROM projects p

                LEFT OUTER JOIN projectpids pp ON (pp.project = p.id)
                LEFT OUTER JOIN dsm_identities i ON (i.pid = pp.pid)

                LEFT OUTER JOIN dsm_services dc ON (dc.id = p.collectedfrom)

                LEFT OUTER JOIN project_fundingpath pf ON (pf.project = p.id)
                LEFT OUTER JOIN fundingpaths fp ON (fp.id = pf.funding)

                LEFT OUTER JOIN project_subject ps ON (ps.project = p.id)
                LEFT OUTER JOIN subjects s ON (s.id = ps.subject)

        GROUP BY
                p.id,
                p.code,
                p.websiteurl,
                p.acronym,
                p.title,
                p.startdate,
                p.enddate,
                p.call_identifier,
                p.keywords,
                p.duration,
                p.ec_sc39,
                p.oa_mandate_for_publications,
                p.ec_article29_3,
                p.dateofcollection,
                p.inferred,
                p.deletedbyinference,
                p.trust,
                p.inferenceprovenance,
                p.contactfullname,
                p.contactfax,
                p.contactphone,
                p.contactemail,
                p.summary,
                p.currency,
                p.totalcost,
                p.fundedamount,
                dc.id,
                dc.officialname,
                p.contracttype,
                p.contracttypescheme;
                