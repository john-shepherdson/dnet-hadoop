CREATE TABLE ${stats_db_name}.otherresearchproduct_concepts AS SELECT substr(p.id, 4) as id, contexts.context.id as concept from ${openaire_db_name}.otherresearchproduct p LATERAL VIEW explode(p.context) contexts as context;
