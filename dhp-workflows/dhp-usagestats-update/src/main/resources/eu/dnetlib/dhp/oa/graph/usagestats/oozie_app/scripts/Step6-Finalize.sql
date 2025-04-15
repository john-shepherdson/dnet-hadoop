-- Drop and create views_stats table
DROP TABLE IF EXISTS ${usagestats_db}.views_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.views_stats
    USING PARQUET
AS
SELECT * FROM ${usagestats_db}.openaire_views_stats_tmp; /*EOS*/

INSERT INTO ${usagestats_db}.views_stats SELECT * FROM ${usagestats_db}.episciences_views_stats; /*EOS*/
INSERT INTO ${usagestats_db}.views_stats SELECT * FROM ${usagestats_db}.pedocs_views_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.views_stats SELECT * FROM ${usagestats_db}.pangaea_views_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.views_stats SELECT * FROM ${usagestats_db}.tudelft_views_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.views_stats SELECT * FROM ${usagestats_db}.la_views_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.views_stats SELECT * FROM ${usagestats_db}.b2share_views_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.views_stats SELECT * FROM ${usagestats_raw_db}.datacite_views; /*EOS*/

-- Drop and create downloads_stats table
DROP TABLE IF EXISTS ${usagestats_db}.downloads_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.downloads_stats
    USING PARQUET
AS
SELECT * FROM ${usagestats_db}.openaire_downloads_stats_tmp; /*EOS*/

INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.episciences_downloads_stats; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.pedocs_downloads_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.pangaea_downloads_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.tudelft_downloads_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.b2share_downloads_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.la_downloads_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.irus_downloads_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats
SELECT source, repository_id, result_id, date, downloads, openaire
FROM ${usagestats_db}.irus_R5_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_db}.sarc_downloads_stats_tmp; /*EOS*/
INSERT INTO ${usagestats_db}.downloads_stats SELECT * FROM ${usagestats_raw_db}.datacite_downloads; /*EOS*/

-- Insert IRUS views to views_stats
INSERT INTO ${usagestats_db}.views_stats
SELECT source, repository_id, result_id, date, views, openaire
FROM ${usagestats_db}.irus_R5_stats_tmp; /*EOS*/

-- Drop and create pageviews_stats
DROP TABLE IF EXISTS ${usagestats_db}.pageviews_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.pageviews_stats
    USING PARQUET
AS
SELECT * FROM ${usagestats_db}.openaire_pageviews_stats_tmp; /*EOS*/

-- usage_stats
DROP TABLE IF EXISTS ${usagestats_db}.usage_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.usage_stats
    USING PARQUET
AS
SELECT
    coalesce(ds.source, vs.source) as source,
    coalesce(ds.repository_id, vs.repository_id) as repository_id,
    coalesce(ds.result_id, vs.result_id) as result_id,
    coalesce(ds.date, vs.date) as date,
  coalesce(ds.count, 0) as downloads,
  coalesce(vs.count, 0) as views,
  coalesce(ds.openaire, 0) as openaire_downloads,
  coalesce(vs.openaire, 0) as openaire_views
FROM ${usagestats_db}.downloads_stats ds
    FULL OUTER JOIN ${usagestats_db}.views_stats vs
ON ds.source = vs.source AND ds.repository_id = vs.repository_id AND ds.result_id = vs.result_id AND ds.date = vs.date; /*EOS*/

-- project_downloads and views
CREATE OR REPLACE VIEW ${usagestats_db}.project_downloads AS
SELECT pr.id, SUM(count) AS downloads, SUM(openaire) AS openaire_downloads, date
FROM ${usagestats_db}.downloads_stats d
    JOIN ${stats_db}.project_results pr ON d.result_id = pr.result
    JOIN ${stats_db}.project p ON p.id = pr.id
GROUP BY pr.id, date; /*EOS*/

CREATE OR REPLACE VIEW ${usagestats_db}.project_views AS
SELECT pr.id, SUM(count) AS views, SUM(openaire) AS openaire_views, date
FROM ${usagestats_db}.views_stats v
    JOIN ${stats_db}.project_results pr ON v.result_id = pr.result
    JOIN ${stats_db}.project p ON p.id = pr.id
GROUP BY pr.id, date; /*EOS*/

-- project_stats
DROP TABLE IF EXISTS ${usagestats_db}.project_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.project_stats
    USING PARQUET
AS
SELECT
    coalesce(pv.id, pd.id) as id,
    coalesce(pd.date, pv.date) as date,
  coalesce(pv.views, 0) as views,
  coalesce(pd.downloads, 0) as downloads,
  coalesce(pv.openaire_views, 0) as openaire_views,
  coalesce(pd.openaire_downloads, 0) as openaire_downloads
FROM ${usagestats_db}.project_downloads pd
    FULL OUTER JOIN ${usagestats_db}.project_views pv
ON pd.id = pv.id AND pd.date = pv.date; /*EOS*/

-- datasource_stats
DROP TABLE IF EXISTS ${usagestats_db}.datasource_stats; /*EOS*/

CREATE TABLE ${usagestats_db}.datasource_stats
    USING PARQUET
AS
    WITH datasource_views AS (
        SELECT repository_id, SUM(views) AS views, SUM(openaire_views) AS openaire_views, date
    FROM ${usagestats_db}.usage_stats
    GROUP BY repository_id, date
    ),
    datasource_downloads AS (
                                SELECT repository_id, SUM(downloads) AS downloads, SUM(openaire_downloads) AS openaire_downloads, date
    FROM ${usagestats_db}.usage_stats
    GROUP BY repository_id, date
    )
SELECT
    coalesce(dv.repository_id, dd.repository_id) AS repository_id,
    coalesce(dd.date, dv.date) AS date,
  coalesce(dv.views, 0) AS views,
  coalesce(dd.downloads, 0) AS downloads,
  coalesce(dv.openaire_views, 0) AS openaire_views,
  coalesce(dd.openaire_downloads, 0) AS openaire_downloads
FROM datasource_downloads dd
    FULL OUTER JOIN datasource_views dv
ON dd.repository_id = dv.repository_id AND dd.date = dv.date; /*EOS*/

-- counter_r5_stats_with_metrics
INSERT INTO ${usagestats_db}.counter_r5_stats_with_metrics
SELECT * FROM ${usagestats_db}.lr_tbl_all_r5_metrics; /*EOS*/

INSERT INTO ${usagestats_db}.counter_r5_stats_with_metrics
SELECT
    s.source,
    d.id AS repository_id,
    ro.id AS result_id,
    CONCAT(YEAR(date), '/', LPAD(MONTH(date), 2, '0')) AS date,
  s.unique_item_investigations,
  s.total_item_investigations,
  s.unique_item_requests,
  s.total_item_requests
FROM ${usagestats_raw_db}.sushilog_cop_r5 s
    JOIN ${stats_db}.datasource_oids d ON s.repository = d.oid
    JOIN ${stats_db}.result_oids ro ON s.rid = ro.oid
WHERE s.source = 'IRUS-UK'; /*EOS*/

-- Drop and create the views in the permanent database
CREATE DATABASE IF NOT EXISTS ${permanent_usagestats_db}; /*EOS*/

-- views_stats
CREATE OR REPLACE VIEW ${openaire_prod_usage_stats}.views_stats AS
SELECT * FROM ${usagestats_db}.views_stats; /*EOS*/

-- pageviews_stats
CREATE OR REPLACE VIEW ${openaire_prod_usage_stats}.pageviews_stats AS
SELECT * FROM ${usagestats_db}.pageviews_stats; /*EOS*/

-- downloads_stats
CREATE OR REPLACE VIEW ${openaire_prod_usage_stats}.downloads_stats AS
SELECT * FROM ${usagestats_db}.downloads_stats; /*EOS*/

-- usage_stats
CREATE OR REPLACE VIEW ${openaire_prod_usage_stats}.usage_stats AS
SELECT * FROM ${usagestats_db}.usage_stats; /*EOS*/

-- project_stats
CREATE OR REPLACE VIEW ${openaire_prod_usage_stats}.project_stats AS
SELECT * FROM ${usagestats_db}.project_stats; /*EOS*/

-- datasource_stats
CREATE OR REPLACE VIEW ${openaire_prod_usage_stats}.datasource_stats AS
SELECT * FROM ${usagestats_db}.datasource_stats; /*EOS*/

-- COUNTER R5 Metrics
CREATE OR REPLACE VIEW ${openaire_prod_usage_stats}.counter_r5_stats_with_metrics AS
SELECT * FROM ${usagestats_db}.counter_r5_stats_with_metrics; /*EOS*/