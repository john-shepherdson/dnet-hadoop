-- Software_citations
Create table ${stats_db_name}.software_citations as select substr(s.id, 4) as id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") as result from ${openaire_db_name}.software s  lateral view explode(s.extrainfo) citations as citation where xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="";
