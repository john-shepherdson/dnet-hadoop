DROP TABLE IF EXISTS ${stats_db_name}.datasource_languages;
CREATE TABLE ${stats_db_name}.datasource_languages AS SELECT substr(d.id, 4) as id, langs.languages as language from openaire.datasource d LATERAL VIEW explode(d.odlanguages.value) langs as languages;
