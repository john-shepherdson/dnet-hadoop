CREATE TABLE IF NOT EXISTS ${usagestats_db}.sarc_downloads_stats_tmp
    (`source` string,
     `repository_id` string,
     `result_id` string,
     `date`	string,
     `count` bigint,
     `openaire`	bigint); /*EOS*/

INSERT INTO ${usagestats_db}.sarc_downloads_stats_tmp
SELECT s.source, d.id AS repository_id, ro.id as result_id, CONCAT(CAST(YEAR(`date`) AS STRING), '/', LPAD(CAST(MONTH(`date`) AS STRING), 2, '0')) AS `date`, s.count, '0'
FROM ${usagestats_raw_db}.sushilog s, ${stats_db}.datasource_oids d, ${stats_db} .result_pids ro
WHERE d.oid LIKE CONCAT('%', s.repository, '%') AND d.id like CONCAT('%', 'sarcservicod', '%') AND s.rid=ro.pid AND ro.type='Digital Object Identifier' AND s.metric_type='ft_total' AND s.source='SARC-OJS'; /*EOS*/