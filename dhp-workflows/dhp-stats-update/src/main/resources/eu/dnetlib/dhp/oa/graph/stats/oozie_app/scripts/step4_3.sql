-- Dataset_citations
Create table ${stats_db_name}.dataset_citations as select substr(d.id, 4) as id, xpath_string(citation.value, "//citation/id[@type='openaire']/@value") as result from ${openaire_db_name}.dataset d  lateral view explode(d.extrainfo) citations as citation where xpath_string(citation.value, "//citation/id[@type='openaire']/@value") !="";
