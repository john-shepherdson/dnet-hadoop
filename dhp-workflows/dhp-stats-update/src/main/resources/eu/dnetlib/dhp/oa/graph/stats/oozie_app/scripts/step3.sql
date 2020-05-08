-- 3. Publication_citations
CREATE TABLE ${stats_db_name}.publication_citations AS SELECT substr(p.id, 4) AS id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") AS result FROM ${openaire_db_name}.publication p lateral view explode(p.extrainfo) citations AS citation WHERE xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="";
