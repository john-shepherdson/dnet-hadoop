DROP TABLE IF EXISTS ${stats_db_name}.datasource_oids;
CREATE TABLE ${stats_db_name}.datasource_oids AS SELECT substr(d.id, 4) as id, oids.ids as oid from ${openaire_db_name}.datasource d LATERAL VIEW explode(d.originalid) oids as ids;
