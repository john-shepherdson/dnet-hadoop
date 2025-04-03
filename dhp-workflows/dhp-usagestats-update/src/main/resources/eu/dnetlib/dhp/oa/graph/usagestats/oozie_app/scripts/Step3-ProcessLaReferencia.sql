CREATE OR REPLACE VIEW ${usagestats_db}.la_result_views_monthly_tmp AS
SELECT entity_id AS id,
       COUNT(entity_id) as views,
       SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
       CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
       source
FROM ${usagestats_raw_db}.lareferencialog
WHERE action='action' and (source_item_type='oaItem' or source_item_type='repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source
ORDER BY source, entity_id; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.la_views_stats_tmp; /*EOS*/
CREATE TABLE IF NOT EXISTS ${usagestats_db}.la_views_stats_tmp AS
SELECT 'LaReferencia' as source, d.id as repository_id, ro.id as result_id, month as date, max(views) AS count, max(openaire_referrer) AS openaire
FROM ${usagestats_db}.la_result_views_monthly_tmp p
JOIN ${stats_db}.datasource_oids d on p.source=d.oid
JOIN ${stats_db} .result_oids ro on p.id=ro.oid
GROUP BY d.id, ro.id, month
ORDER BY d.id, ro.id, month; /*EOS*/

CREATE OR REPLACE VIEW ${usagestats_db}.la_result_downloads_monthly_tmp AS
SELECT entity_id AS id,
       COUNT(entity_id) as downloads,
       SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
       CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
       source
FROM ${usagestats_raw_db}.lareferencialog
WHERE action='download' and (source_item_type='oaItem' or source_item_type='repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source
ORDER BY source, entity_id; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.la_downloads_stats_tmp; /*EOS*/
CREATE TABLE IF NOT EXISTS ${usagestats_db}.la_downloads_stats_tmp AS
SELECT 'LaReferencia' as source, d.id as repository_id, ro.id as result_id, month as date, max(downloads) AS count, max(openaire_referrer) AS openaire
FROM ${usagestats_db}.la_result_downloads_monthly_tmp p
JOIN ${stats_db}.datasource_oids d on p.source=d.oid
JOIN ${stats_db}.result_oids ro on p.id=ro.oid
GROUP BY d.id, ro.id, month
ORDER BY d.id, ro.id, month; /*EOS*/