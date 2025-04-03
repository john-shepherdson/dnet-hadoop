DROP TABLE IF EXISTS ${usagestats_db}.views_stats; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.downloads_stats; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.pageviews_stats; /*EOS*/
DROP TABLE IF EXISTS ${usagestats_db}.usage_stats; /*EOS*/

CREATE TABLE IF NOT EXISTS ${usagestats_db}.views_stats STORED AS PARQUET AS
SELECT * FROM ${usagestats_db}.openaire_views_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.pedocs_views_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.tudelft_views_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.la_views_stats_tmp; /*EOS*/

CREATE TABLE IF NOT EXISTS ${usagestats_db}.downloads_stats STORED AS PARQUET AS
SELECT * FROM ${usagestats_db}.openaire_downloads_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.pedocs_downloads_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.tudelft_downloads_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.la_downloads_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.irus_downloads_stats_tmp
UNION ALL
SELECT * FROM ${usagestats_db}.sarc_downloads_stats_tmp; /*EOS*/


CREATE TABLE IF NOT EXISTS ${usagestats_db}.pageviews_stats STORED AS PARQUET AS
SELECT * FROM ${usagestats_db}.openaire_pageviews_stats_tmp; /*EOS*/

DROP TABLE IF EXISTS ${usagestats_db}.full_dates; /*EOS*/

-- Create a temporary table to generate a sequence of months
WITH month_seq AS (
    SELECT explode(sequence(
            to_date('2016-01-01', 'yyyy-MM-dd'),
            current_date(),
            interval 1 month
    )) AS txn_date
)
-- Create the full_dates table if not exists
CREATE TABLE IF NOT EXISTS ${usagestats_db}.full_dates AS
SELECT date_format(txn_date, 'yyyy/MM') AS txn_date
FROM month_seq; /*EOS*/

CREATE TABLE IF NOT EXISTS ${usagestats_db}.usage_stats AS
SELECT coalesce(ds.source, vs.source) as source,
       coalesce(ds.repository_id, vs.repository_id) as repository_id,
       coalesce(ds.result_id, vs.result_id) as result_id, coalesce(ds.date, vs.date) as date,
       coalesce(ds.count, 0) as downloads, coalesce(vs.count, 0) as views,
       coalesce(ds.openaire, 0) as openaire_downloads,
       coalesce(vs.openaire, 0) as openaire_views
FROM ${usagestats_db}.downloads_stats AS ds
FULL OUTER JOIN ${usagestats_db}.views_stats AS vs ON ds.source=vs.source AND ds.repository_id=vs.repository_id AND ds.result_id=vs.result_id AND ds.date=vs.date; /*EOS*/

-- Create the permanent views
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.views_stats AS SELECT * FROM ${usagestats_db}.views_stats; /*EOS*/
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.pageviews_stats AS SELECT * FROM ${usagestats_db}.pageviews_stats; /*EOS*/
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.downloads_stats AS SELECT * FROM ${usagestats_db}.downloads_stats; /*EOS*/
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.usage_stats AS SELECT * FROM ${usagestats_db}.usage_stats; /*EOS*/