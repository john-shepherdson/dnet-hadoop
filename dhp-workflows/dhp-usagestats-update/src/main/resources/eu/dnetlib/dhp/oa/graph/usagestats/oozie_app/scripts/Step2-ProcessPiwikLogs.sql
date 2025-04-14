-- Drop existing piwiklogdistinct table if it exists
DROP TABLE IF EXISTS ${usagestats_db}.piwiklogdistinct; /*EOS*/

-- Create and populate piwiklogdistinct using Parquet format
CREATE TABLE ${usagestats_db}.piwiklogdistinct
    STORED AS PARQUET AS
SELECT DISTINCT
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
FROM ${usagestats_raw_db}.piwiklog
WHERE entity_id IS NOT NULL
  AND TO_TIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ss') >= ADD_MONTHS(CURRENT_TIMESTAMP(), -24); /*EOS*/



CREATE OR REPLACE VIEW ${usagestats_db}.openaire_result_views_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS views,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'action'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Drop and create the temporary views stats table
DROP TABLE IF EXISTS ${usagestats_db}.openaire_views_stats_tmp; /*EOS*/

CREATE TABLE ${usagestats_db}.openaire_views_stats_tmp AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Handle missing piwik_id mappings
-- 630
INSERT INTO ${usagestats_db}.openaire_views_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::cfa5301358b9fcbe7aa45b1ceea088c6', ro.id, month, MAX(views), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 630 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

-- 662
INSERT INTO ${usagestats_db}.openaire_views_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::4e86eaf2685a67b743a475f86c7c0086', ro.id, month, MAX(views), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 662 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

-- 694
INSERT INTO ${usagestats_db}.openaire_views_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::f35fd567065af297ae65b621e0a21ae9', ro.id, month, MAX(views), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 694 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

-- 725
INSERT INTO ${usagestats_db}.openaire_views_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::7180cffd6a8e829dacfc2a31b3f72ece', ro.id, month, MAX(views), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 725 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

-- 728
INSERT INTO ${usagestats_db}.openaire_views_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::8b3bac12926cc1d9fb5d68783376971d', ro.id, month, MAX(views), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 728 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

-- Create the pageviews stats table (for portal only)
CREATE TABLE ${usagestats_db}.openaire_pageviews_stats_tmp AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count
    FROM ${usagestats_db}.openaire_result_views_monthly_tmp p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.id
    WHERE p.source = ${portal_matomo_id}
    AND ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Cleanup
DROP VIEW IF EXISTS ${usagestats_db}.openaire_result_views_monthly_tmp; /*EOS*/


-- Drop and create openaire_result_downloads_monthly_tmp as TEMP VIEW
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.openaire_result_downloads_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS downloads,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'download'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Drop and create downloads stats table
DROP TABLE IF EXISTS ${usagestats_db}.openaire_downloads_stats_tmp; /*EOS*/

CREATE TABLE ${usagestats_db}.openaire_downloads_stats_tmp AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(downloads) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.openaire_result_downloads_monthly_tmp p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Insert missing piwik_id mappings
INSERT INTO ${usagestats_db}.openaire_downloads_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::cfa5301358b9fcbe7aa45b1ceea088c6', ro.id, month, MAX(downloads), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_downloads_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 630 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

INSERT INTO ${usagestats_db}.openaire_downloads_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::4e86eaf2685a67b743a475f86c7c0086', ro.id, month, MAX(downloads), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_downloads_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 662 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

INSERT INTO ${usagestats_db}.openaire_downloads_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::f35fd567065af297ae65b621e0a21ae9', ro.id, month, MAX(downloads), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_downloads_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 694 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

INSERT INTO ${usagestats_db}.openaire_downloads_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::7180cffd6a8e829dacfc2a31b3f72ece', ro.id, month, MAX(downloads), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_downloads_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 725 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

INSERT INTO ${usagestats_db}.openaire_downloads_stats_tmp
SELECT 'OpenAIRE', 'opendoar____::8b3bac12926cc1d9fb5d68783376971d', ro.id, month, MAX(downloads), MAX(openaire_referrer)
FROM ${usagestats_db}.openaire_result_downloads_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
WHERE p.source = 728 AND ro.oid NOT IN ('200', '204', '404', '400', '503')
GROUP BY ro.id, month; /*EOS*/

-- Cleanup
DROP VIEW IF EXISTS ${usagestats_db}.openaire_result_downloads_monthly_tmp; /*EOS*/




-- Unique Item Investigations
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.view_unique_item_investigations AS
SELECT
    id_visit,
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    1 AS unique_item_investigations,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE (source_item_type = 'oaItem' OR source_item_type = 'repItem') AND entity_id IS NOT NULL
GROUP BY id_visit, entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.tbl_unique_item_investigations; /*EOS*/

CREATE TABLE ${usagestats_db}.tbl_unique_item_investigations AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    SUM(unique_item_investigations) AS unique_item_investigations,
    SUM(openaire_referrer) AS openaire
    FROM ${usagestats_db}.view_unique_item_investigations p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Total Item Investigations
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.view_total_item_investigations AS
SELECT
    id_visit,
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS total_item_investigations,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE (source_item_type = 'oaItem' OR source_item_type = 'repItem') AND entity_id IS NOT NULL
GROUP BY id_visit, entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.tbl_total_item_investigations; /*EOS*/

CREATE TABLE ${usagestats_db}.tbl_total_item_investigations AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    SUM(total_item_investigations) AS total_item_investigations,
    SUM(openaire_referrer) AS openaire
    FROM ${usagestats_db}.view_total_item_investigations p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Unique Item Requests
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.view_unique_item_requests AS
SELECT
    id_visit,
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    1 AS unique_item_requests,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'download' AND (source_item_type = 'oaItem' OR source_item_type = 'repItem') AND entity_id IS NOT NULL
GROUP BY id_visit, entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.tbl_unique_item_requests; /*EOS*/

CREATE TABLE ${usagestats_db}.tbl_unique_item_requests AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    SUM(unique_item_requests) AS unique_item_requests,
    SUM(openaire_referrer) AS openaire
    FROM ${usagestats_db}.view_unique_item_requests p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Total Item Requests
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.view_total_item_requests AS
SELECT
    id_visit,
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS total_item_requests,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'download' AND (source_item_type = 'oaItem' OR source_item_type = 'repItem') AND entity_id IS NOT NULL
GROUP BY id_visit, entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.tbl_total_item_requests; /*EOS*/

CREATE TABLE ${usagestats_db}.tbl_total_item_requests AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    SUM(total_item_requests) AS total_item_requests,
    SUM(openaire_referrer) AS openaire
    FROM ${usagestats_db}.view_total_item_requests p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Final CoP R5 metrics
DROP TABLE IF EXISTS ${usagestats_db}.tbl_all_r5_metrics; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.counter_r5_stats_with_metrics; /*EOS*/

CREATE TABLE ${usagestats_db}.counter_r5_stats_with_metrics AS
WITH tmp1 AS (
    SELECT
        COALESCE(ds.repository_id, vs.repository_id) AS repository_id,
        COALESCE(ds.result_id, vs.result_id) AS result_id,
        COALESCE(ds.date, vs.date) AS date,
    COALESCE(vs.unique_item_investigations, 0) AS unique_item_investigations,
    COALESCE(ds.total_item_investigations, 0) AS total_item_investigations
    FROM ${usagestats_db}.tbl_unique_item_investigations vs
    FULL OUTER JOIN ${usagestats_db}.tbl_total_item_investigations ds
    ON ds.source = vs.source AND ds.result_id = vs.result_id AND ds.date = vs.date
    ),
    tmp2 AS (
                SELECT
                COALESCE(ds.repository_id, vs.repository_id) AS repository_id,
    COALESCE(ds.result_id, vs.result_id) AS result_id,
    COALESCE(ds.date, vs.date) AS date,
    COALESCE(ds.total_item_investigations, 0) AS total_item_investigations,
    COALESCE(ds.unique_item_investigations, 0) AS unique_item_investigations,
    COALESCE(vs.unique_item_requests, 0) AS unique_item_requests
    FROM tmp1 ds
    FULL OUTER JOIN ${usagestats_db}.tbl_unique_item_requests vs
    ON ds.repository_id = vs.repository_id AND ds.result_id = vs.result_id AND ds.date = vs.date
    )
SELECT
    'OpenAIRE' AS source,
    COALESCE(ds.repository_id, vs.repository_id) AS repository_id,
    COALESCE(ds.result_id, vs.result_id) AS result_id,
    COALESCE(ds.date, vs.date) AS date,
    COALESCE(ds.unique_item_investigations, 0) AS unique_item_investigations,
    COALESCE(ds.total_item_investigations, 0) AS total_item_investigations,
    COALESCE(ds.unique_item_requests, 0) AS unique_item_requests,
    COALESCE(vs.total_item_requests, 0) AS total_item_requests
FROM tmp2 ds
    FULL OUTER JOIN ${usagestats_db}.tbl_total_item_requests vs
ON ds.repository_id = vs.repository_id AND ds.result_id = vs.result_id AND ds.date = vs.date; /*EOS*/




-- STEP 1: Create Episciences distinct table
DROP TABLE IF EXISTS ${usagestats_db}.episcienceslogdistinct; /*EOS*/

CREATE TABLE ${usagestats_db}.episcienceslogdistinct AS
SELECT DISTINCT *
FROM ${usagestats_raw_db}.episcienceslog
WHERE entity_id IS NOT NULL; /*EOS*/

-- STEP 2: Views stats
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.episciences_views_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS views,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.episcienceslogdistinct
WHERE action = 'action'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.episciences_views_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.episciences_views_stats AS
SELECT
    'Episciences' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.episciences_views_monthly_tmp p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- STEP 3: Downloads stats
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.episciences_downloads_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS downloads,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.episcienceslogdistinct
WHERE action = 'download'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.episciences_downloads_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.episciences_downloads_stats AS
SELECT
    'Episciences' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(downloads) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.episciences_downloads_monthly_tmp p
    JOIN ${stats_db}.datasource d ON p.source = d.piwik_id
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    WHERE ro.oid NOT IN ('200', '204', '404', '400', '503')
    AND d.id != 're3data_____::7b0ad08687b2c960d5aeef06f811d5e6'
    GROUP BY d.id, ro.id, month; /*EOS*/




-- Drop Pedocs views stats table
DROP TABLE IF EXISTS ${usagestats_db}.pedocs_views_stats_tmp; /*EOS*/

-- Drop Pedocs downloads stats table
DROP TABLE IF EXISTS ${usagestats_db}.pedocs_downloads_stats_tmp; /*EOS*/

-- Create Pedocs views stats table
CREATE TABLE ${usagestats_db}.pedocs_views_stats_tmp AS
SELECT
    'OpenAIRE' AS source,
    'opendoar____::ab1a4d0dd4d48a2ba1077c4494791306' AS repository_id,
    r.id AS result_id,
    p.date,
    p.counter_abstract AS count,
    0 AS openaire
FROM ${usagestats_raw_db}.pedocsoldviews p
    JOIN ${stats_db}.result_oids r ON r.oid = p.identifier; /*EOS*/

-- Create Pedocs downloads stats table
CREATE TABLE ${usagestats_db}.pedocs_downloads_stats_tmp AS
SELECT
    'OpenAIRE' AS source,
    'opendoar____::ab1a4d0dd4d48a2ba1077c4494791306' AS repository_id,
    r.id AS result_id,
    p.date,
    p.counter AS count,
    0 AS openaire
FROM ${usagestats_raw_db}.pedocsolddownloads p
    JOIN ${stats_db}.result_oids r ON r.oid = p.identifier; /*EOS*/



-- Drop Pangaea views stats table
DROP TABLE IF EXISTS ${usagestats_db}.pangaea_views_stats_tmp; /*EOS*/

-- Drop Pangaea downloads stats table
DROP TABLE IF EXISTS ${usagestats_db}.pangaea_downloads_stats_tmp; /*EOS*/

-- Create Pangaea views stats table
CREATE TABLE ${usagestats_db}.pangaea_views_stats_tmp AS
SELECT
    'PANGAEA' AS source,
    're3data_____::9633d1e8c4309c833c2c442abeb0cfeb' AS repository_id,
    r.id AS result_id,
    p.date,
    CAST(p.count AS BIGINT) AS count,
    0 AS openaire
FROM default.pangaeaviews p
JOIN ${stats_db}.result_oids r ON r.oid = p.result_id; /*EOS*/

-- Create Pangaea downloads stats table
CREATE TABLE ${usagestats_db}.pangaea_downloads_stats_tmp AS
SELECT
    'PANGAEA' AS source,
    're3data_____::9633d1e8c4309c833c2c442abeb0cfeb' AS repository_id,
    r.id AS result_id,
    p.date,
    CAST(p.count AS BIGINT) AS count,
    0 AS openaire
FROM default.pangaeadownloads p
JOIN ${stats_db}.result_oids r ON r.oid = p.result_id; /*EOS*/




-- Drop TUDELFT views & downloads monthly temp views
DROP VIEW IF EXISTS ${usagestats_db}.tudelft_result_views_monthly_tmp; /*EOS*/
DROP VIEW IF EXISTS ${usagestats_db}.tudelft_result_downloads_monthly_tmp; /*EOS*/

-- Drop stats tables if they exist
DROP TABLE IF EXISTS ${usagestats_db}.tudelft_views_stats_tmp; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.tudelft_downloads_stats_tmp; /*EOS*/

-- Create TUDELFT views monthly temp view
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.tudelft_result_views_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS views,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'action'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
  AND source = 252
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Create TUDELFT views stats table
CREATE TABLE ${usagestats_db}.tudelft_views_stats_tmp AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.tudelft_result_views_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON CONCAT('tud:', p.id) = ro.oid
    JOIN ${stats_db}.datasource d ON d.id = 'opendoar____::c9892a989183de32e976c6f04e700201'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Create TUDELFT downloads monthly temp view
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.tudelft_result_downloads_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS views,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'download'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
  AND source = 252
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Create TUDELFT downloads stats table
CREATE TABLE ${usagestats_db}.tudelft_downloads_stats_tmp AS
SELECT
    'OpenAIRE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.tudelft_result_downloads_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON CONCAT('tud:', p.id) = ro.oid
    JOIN ${stats_db}.datasource d ON d.id = 'opendoar____::c9892a989183de32e976c6f04e700201'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Drop TUDELFT temp views (cleanup)
DROP VIEW IF EXISTS ${usagestats_db}.tudelft_result_views_monthly_tmp; /*EOS*/
DROP VIEW IF EXISTS ${usagestats_db}.tudelft_result_downloads_monthly_tmp; /*EOS*/




-- Drop temp views if they exist
DROP VIEW IF EXISTS ${usagestats_db}.b2share_result_views_monthly_tmp; /*EOS*/
DROP VIEW IF EXISTS ${usagestats_db}.b2share_result_downloads_monthly_tmp; /*EOS*/

-- Drop stats tables if they exist
DROP TABLE IF EXISTS ${usagestats_db}.b2share_views_stats_tmp; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.b2share_downloads_stats_tmp; /*EOS*/

-- Create temp view for B2SHARE views
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.b2share_result_views_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS views,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'action'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
  AND source = 412
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Create stats table for B2SHARE views
CREATE TABLE ${usagestats_db}.b2share_views_stats_tmp AS
SELECT
    'B2SHARE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.b2share_result_views_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    JOIN ${stats_db}.datasource d ON d.id = 're3data_____::ad3609c351bd520edf6f10f5e0d9b877'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Create temp view for B2SHARE downloads
CREATE OR REPLACE TEMP VIEW ${usagestats_db}.b2share_result_downloads_monthly_tmp AS
SELECT
    entity_id,
    reflect('java.net.URLDecoder', 'decode', entity_id) AS id,
    COUNT(entity_id) AS views,
    SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer,
    CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month,
    source
FROM ${usagestats_db}.piwiklogdistinct
WHERE action = 'download'
  AND (source_item_type = 'oaItem' OR source_item_type = 'repItem')
  AND source = 412
GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source; /*EOS*/

-- Create stats table for B2SHARE downloads
CREATE TABLE ${usagestats_db}.b2share_downloads_stats_tmp AS
SELECT
    'B2SHARE' AS source,
    d.id AS repository_id,
    ro.id AS result_id,
    month AS date,
    MAX(views) AS count,
    MAX(openaire_referrer) AS openaire
    FROM ${usagestats_db}.b2share_result_downloads_monthly_tmp p
    JOIN ${stats_db}.result_oids ro ON p.id = ro.oid
    JOIN ${stats_db}.datasource d ON d.id = 're3data_____::ad3609c351bd520edf6f10f5e0d9b877'
    GROUP BY d.id, ro.id, month; /*EOS*/

-- Drop temp views (cleanup)
DROP VIEW IF EXISTS ${usagestats_db}.b2share_result_views_monthly_tmp; /*EOS*/
DROP VIEW IF EXISTS ${usagestats_db}.b2share_result_downloads_monthly_tmp; /*EOS*/