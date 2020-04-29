-- Otherresearchproduct_citations
Create table ${stats_db_name}.otherresearchproduct_citations as select substr(o.id, 4) as id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") as result from ${openaire_db_name}.otherresearchproduct o  lateral view explode(o.extrainfo) citations as citation where xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="";
