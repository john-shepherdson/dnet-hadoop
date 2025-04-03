CREATE TABLE IF NOT EXISTS ${usagestats_db}.irus_downloads_stats_tmp
    (`source` string,
     `repository_id` string,
     `result_id` string,
     `date`	string,
     `count` bigint,
     `openaire`	bigint); /*EOS*/

INSERT INTO ${usagestats_db}.irus_downloads_stats_tmp
SELECT s.source, d.id AS repository_id, ro.id as result_id, CONCAT(YEAR(date), '/', LPAD(MONTH(date), 2, '0')) as date, s.count, '0'
FROM ${usagestats_raw_db}.sushilog s
JOIN ${stats_db}.datasource_oids d on s.repository=d.oid
JOIN ${stats_db}.result_oids ro on s.rid=ro.oid
WHERE metric_type='ft_total' AND s.source='IRUS-UK'; /*EOS*/