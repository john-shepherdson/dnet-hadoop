-----------------
-- VIEWS STATS --
-----------------

DROP VIEW IF EXISTS ${usagestats_db}.openaire_piwikresult_views_monthly_tmp; /*EOS*/

CREATE OR REPLACE VIEW ${usagestats_db}.openaire_result_views_monthly_tmp AS
SELECT entity_id,
       reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
       COUNT(entity_id) as views,
       SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
       CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
       source
FROM ${usagestats_raw_db}.piwiklog
WHERE action='action' and (source_item_type='oaItem' or source_item_type='repItem') AND entity_id RLIKE '^([A-Za-z0-9._~\\-]|(%[0-9A-Fa-f]{2}))*$'
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')),
source ORDER BY source, entity_id; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.openaire_views_stats_tmp; /*EOS*/
CREATE TABLE IF NOT EXISTS ${usagestats_db}.openaire_views_stats_tmp AS
SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, max(views) AS count, max(openaire_referrer) AS openaire
FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
JOIN ${stats_db}.datasource d on p.source=d.piwik_id
JOIN ${stats_db}.result_oids ro on p.id=ro.oid
WHERE ro.oid!='200'
GROUP BY d.id, ro.id, month
ORDER BY d.id, ro.id, month; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.openaire_pageviews_stats_tmp; /*EOS*/
CREATE TABLE IF NOT EXISTS ${usagestats_db}.openaire_pageviews_stats_tmp AS
SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, max(views) AS count
FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
JOIN ${stats_db}.datasource d on p.source=d.piwik_id AND p.source=${portalMatomoID}
JOIN ${stats_db}.result_oids ro on p.id=ro.oid
WHERE ro.oid!='200'
GROUP BY d.id, ro.id, month
ORDER BY d.id, ro.id, month; /*EOS*/

---------------------
-- DOWNLOADS STATS --
---------------------
DROP VIEW IF EXISTS ${usagestats_db}.openaire_result_downloads_monthly_tmp; /*EOS*/
CREATE OR REPLACE VIEW ${usagestats_db}.openaire_result_downloads_monthly_tmp AS
SELECT entity_id,
       reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
       COUNT(entity_id) as downloads,
       SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
       CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
       source
FROM ${usagestats_raw_db}.piwiklog
WHERE action='download' AND (source_item_type='oaItem' OR source_item_type='repItem') AND entity_id RLIKE '^([A-Za-z0-9._~\\-]|(%[0-9A-Fa-f]{2}))*$'
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) , source
ORDER BY source, entity_id, month; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.openaire_downloads_stats_tmp; /*EOS*/
CREATE TABLE IF NOT EXISTS ${usagestats_db}.openaire_downloads_stats_tmp AS
SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, max(downloads) AS count, max(openaire_referrer) AS openaire
FROM ${usagestats_db}.openaire_result_downloads_monthly_tmp p
JOIN ${stats_db}.datasource d on p.source=d.piwik_id
JOIN ${stats_db}.result_oids ro on p.id=ro.oid
WHERE ro.oid!='200'
GROUP BY d.id, ro.id, month
ORDER BY d.id, ro.id, month; /*EOS*/

DROP VIEW IF EXISTS ${usagestats_db}.openaire_result_downloads_monthly_tmp; /*EOS*/

----------------
-- OLD PEDOCS --
----------------
DROP TABLE IF EXISTS ${usagestats_db}.pedocs_views_stats_tmp; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.pedocs_downloads_stats; /*EOS*/

CREATE TABLE IF NOT EXISTS ${usagestats_db}.pedocs_views_stats_tmp AS
SELECT 'OpenAIRE' as source, 'opendoar____::ab1a4d0dd4d48a2ba1077c4494791306' as repository_id, r.id as result_id, date, counter_abstract as count, 0 as openaire
FROM ${usagestats_raw_db}.pedocsoldviews p
JOIN ${stats_db}.result_oids r ON r.oid=p.identifier; /*EOS*/

CREATE TABLE IF NOT EXISTS ${usagestats_db}.pedocs_downloads_stats_tmp AS
SELECT 'OpenAIRE' as source, 'opendoar____::ab1a4d0dd4d48a2ba1077c4494791306' as repository_id, r.id as result_id, date, counter as count, 0 as openaire
FROM ${usagestats_raw_db}.pedocsolddownloads p
JOIN ${stats_db}.result_oids r ON r.oid=p.identifier; /*EOS*/

-------------
-- TUDELFT --
-------------
DROP view IF EXISTS ${usagestats_db}.tudelft_result_views_monthly_tmp; /*EOS*/
DROP view IF EXISTS ${usagestats_db}.tudelft_result_downloads_monthly_tmp; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.tudelft_views_stats_tmp; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.tudelft_downloads_stats_tmp; /*EOS*/

CREATE OR REPLACE VIEW ${usagestats_db}.tudelft_result_views_monthly_tmp AS
SELECT entity_id,
       reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
       COUNT(entity_id) as views,
       SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
       CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
       source
FROM ${usagestats_raw_db}.piwiklog
WHERE action='action' and (source_item_type='oaItem' or source_item_type='repItem') and source=252 AND entity_id RLIKE '^([A-Za-z0-9._~\\-]|(%[0-9A-Fa-f]{2}))*$'
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source
ORDER BY source, entity_id; /*EOS*/

CREATE TABLE IF NOT EXISTS ${usagestats_db}.tudelft_views_stats_tmp AS
SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, max(views) AS count, max(openaire_referrer) AS openaire
FROM ${usagestats_db}.tudelft_result_views_monthly_tmp p
JOIN ${stats_db}.datasource d on p.source=d.piwik_id AND d.id='opendoar____::c9892a989183de32e976c6f04e700201'
JOIN ${stats_db}.result_oids ro on concat('tud:',p.id)=ro.oid
GROUP BY d.id, ro.id, month
ORDER BY d.id, ro.id; /*EOS*/

CREATE OR REPLACE VIEW ${usagestats_db}.tudelft_result_downloads_monthly_tmp AS
SELECT entity_id,
       reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
       COUNT(entity_id) as views,
       SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
       CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
       source
FROM ${usagestats_raw_db}.piwiklog
WHERE action='download' and (source_item_type='oaItem' or source_item_type='repItem') and source=252 AND entity_id RLIKE '^([A-Za-z0-9._~\\-]|(%[0-9A-Fa-f]{2}))*$'
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source
ORDER BY source, entity_id; /*EOS*/

CREATE TABLE IF NOT EXISTS ${usagestats_db}.tudelft_downloads_stats_tmp AS
SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, max(views) AS count, max(openaire_referrer) AS openaire
FROM ${usagestats_db}.tudelft_result_downloads_monthly_tmp p
JOIN ${stats_db}.datasource d on p.source=d.piwik_id AND d.id='opendoar____::c9892a989183de32e976c6f04e700201'
JOIN ${stats_db}.result_oids ro on concat('tud:',p.id)=ro.oid
GROUP BY d.id, ro.id, month
ORDER BY d.id, ro.id; /*EOS*/

DROP view IF EXISTS ${usagestats_db}.tudelft_result_views_monthly_tmp; /*EOS*/
DROP view IF EXISTS ${usagestats_db}.tudelft_result_downloads_monthly_tmp; /*EOS*/