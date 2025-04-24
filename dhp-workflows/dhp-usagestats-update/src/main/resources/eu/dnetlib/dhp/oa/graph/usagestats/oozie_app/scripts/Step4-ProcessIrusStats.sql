SET spark.sql.parquet.writer.version = v1; /*EOS*/

-- IRUS Downloads Stats
DROP TABLE IF EXISTS ${usagestats_db}.irus_downloads_stats_tmp; /*EOS*/

CREATE TABLE ${usagestats_db}.irus_downloads_stats_tmp STORED AS PARQUET AS
SELECT
    s.source,
    d.id AS repository_id,
    ro.id AS result_id,
    CONCAT(YEAR(date), '/', LPAD(MONTH(date), 2, '0')) AS date,
    s.count,
    0 AS openaire
FROM ${usagestats_raw_db}.sushilog s
JOIN ${stats_db}.datasource_oids d ON s.repository = d.oid
JOIN ${stats_db}.result_oids ro ON s.rid = ro.oid
WHERE s.metric_type = 'ft_total' AND s.source = 'IRUS-UK'; /*EOS*/

-- IRUS CoP R5 Stats
DROP TABLE IF EXISTS ${usagestats_db}.irus_r5_stats_tmp; /*EOS*/

CREATE TABLE ${usagestats_db}.irus_r5_stats_tmp STORED AS PARQUET AS
SELECT
    s.source,
    d.id AS repository_id,
    ro.id AS result_id,
    CONCAT(YEAR(date), '/', LPAD(MONTH(date), 2, '0')) AS date,
    (s.total_item_investigations - s.total_item_requests) AS views,
    s.total_item_requests AS downloads,
    0 AS openaire
FROM ${usagestats_raw_db}.sushilog_cop_r5 s
JOIN ${stats_db}.datasource_oids d ON s.repository = d.oid
JOIN ${stats_db}.result_oids ro ON s.rid = ro.oid
WHERE s.source = 'IRUS-UK'; /*EOS*/