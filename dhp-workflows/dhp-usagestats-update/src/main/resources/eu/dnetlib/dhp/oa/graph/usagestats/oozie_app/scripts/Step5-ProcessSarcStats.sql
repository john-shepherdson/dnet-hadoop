DROP TABLE IF EXISTS ${usagestats_db}.sarc_downloads_stats_tmp; /*EOS*/

CREATE TABLE ${usagestats_db}.sarc_downloads_stats_tmp STORED AS PARQUET AS
SELECT
    s.source,
    d.id AS repository_id,
    ro.id AS result_id,
    CONCAT(CAST(YEAR(date) AS STRING), '/', LPAD(CAST(MONTH(date) AS STRING), 2, '0')) AS date,
    s.count,
    0 AS openaire
FROM ${usagestats_raw_db}.sushilog s
JOIN ${stats_db}.datasource_oids d ON d.oid LIKE CONCAT('%', s.repository, '%') AND d.id LIKE '%sarcservicod%'
JOIN ${stats_db}.result_pids ro ON s.rid = ro.pid AND ro.type = 'Digital Object Identifier'
WHERE s.metric_type = 'ft_total' AND s.source = 'SARC-OJS'; /*EOS*/