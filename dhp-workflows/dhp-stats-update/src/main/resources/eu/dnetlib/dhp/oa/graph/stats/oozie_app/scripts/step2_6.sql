CREATE TABLE ${stats_db_name}.publication_languages AS select substr(p.id, 4) as id, p.language.classname as language from ${openaire_db_name}.publication p;
