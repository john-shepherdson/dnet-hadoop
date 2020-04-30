CREATE TABLE ${stats_db_name}.software_oids AS SELECT substr(p.id, 4) as id, oids.ids as oid from ${openaire_db_name}.software p LATERAL VIEW explode(p.originalid) oids as ids;
