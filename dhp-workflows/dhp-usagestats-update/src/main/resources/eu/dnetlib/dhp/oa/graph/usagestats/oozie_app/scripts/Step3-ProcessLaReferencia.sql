-- Create and populate lareferencialogdistinct in one step
CREATE TABLE ${usagestats_db}.lareferencialogdistinct
    USING PARQUET AS
SELECT DISTINCT
    matomoid,
    source,
    id_visit,
    country,
    action,
    url,
    entity_id,
    source_item_type,
    timestamp,
    referrer_name,
    agent
FROM ${usagerawdata_db}.lareferencialog
WHERE entity_id IS NOT NULL; /*EOS*/

-- Create temporary view for monthly view aggregation
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.la_result_views_monthly_tmp AS
SELECT
    entity_id AS id,
    COUNT(entity_id) AS views,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.lareferencialogdistinct
WHERE action = 'action'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Drop previous stats table if it exists
DROP TABLE IF EXISTS ${usagestats_db}.la_views_stats_tmp; /*EOS*/

-- Create new stats table with aggregated values
CREATE TABLE ${usagestats_db}.la_views_stats_tmp AS
SELECT
    'LaReferencia' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.la_result_views_monthly_tmp p
    JOIN ${stats_db}.datasource_oids d ON p.source = d.oid
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Create temporary view for monthly downloads aggregation
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.la_result_downloads_monthly_tmp AS
SELECT
    entity_id AS id,
    COUNT(entity_id) AS downloads,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.lareferencialogdistinct
WHERE action = 'download'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Drop old stats table if it exists
DROP TABLE IF EXISTS ${usagestats_db}.la_downloads_stats_tmp; /*EOS*/

-- Create stats table from the temp view
CREATE TABLE ${usagestats_db}.la_downloads_stats_tmp AS
SELECT
    'LaReferencia' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(downloads) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.la_result_downloads_monthly_tmp p
    JOIN ${stats_db}.datasource_oids d ON p.source = d.oid
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Unique Item Investigations
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.lr_view_unique_item_investigations AS
SELECT
    id_visit,
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    1 AS unique_item_investigations,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.lareferencialogdistinct
WHERE source_item_type IN ('oaItem', 'repItem') AND entity_id IS NOT NULL
GROUP BY id_visit, entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.lr_tbl_unique_item_investigations; /*EOS*/

CREATE TABLE ${usagestats_db}.lr_tbl_unique_item_investigations AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    SUM(unique_item_investigations) AS unique_item_investigations,
    SUM(openaire_referrer) AS openaire
    FROM ${usagestats_db}.lr_view_unique_item_investigations p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '400', '404', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Total Item Investigations
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.lr_view_total_item_investigations AS
SELECT
    id_visit,
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS total_item_investigations,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.lareferencialogdistinct
WHERE source_item_type IN ('oaItem', 'repItem') AND entity_id IS NOT NULL
GROUP BY id_visit, entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.lr_tbl_total_item_investigations; /*EOS*/

CREATE TABLE ${usagestats_db}.lr_tbl_total_item_investigations AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    SUM(total_item_investigations) AS total_item_investigations,
    SUM(openaire_referrer) AS openaire
    FROM ${usagestats_db}.lr_view_total_item_investigations p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '400', '404', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/