CREATE TABLE ${stats_db_name}.publication_oids AS SELECT substr(p.id, 4) as id, oids.ids as oid from ${openaire_db_name}.publication p LATERAL VIEW explode(p.originalid) oids as ids;
