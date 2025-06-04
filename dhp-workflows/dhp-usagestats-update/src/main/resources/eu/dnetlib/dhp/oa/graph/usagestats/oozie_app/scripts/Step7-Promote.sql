-- Drop and create the views in the permanent database
CREATE DATABASE IF NOT EXISTS ${permanent_usagestats_db}; /*EOS*/

-- views_stats
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.views_stats AS
SELECT * FROM ${usagestats_db}.views_stats; /*EOS*/

-- pageviews_stats
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.pageviews_stats AS
SELECT * FROM ${usagestats_db}.pageviews_stats; /*EOS*/

-- downloads_stats
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.downloads_stats AS
SELECT * FROM ${usagestats_db}.downloads_stats; /*EOS*/

-- usage_stats
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.usage_stats AS
SELECT * FROM ${usagestats_db}.usage_stats; /*EOS*/

-- project_stats
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.project_stats AS
SELECT * FROM ${usagestats_db}.project_stats; /*EOS*/

-- datasource_stats
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.datasource_stats AS
SELECT * FROM ${usagestats_db}.datasource_stats; /*EOS*/

-- COUNTER R5 Metrics
CREATE OR REPLACE VIEW ${permanent_usagestats_db}.counter_r5_stats_with_metrics AS
SELECT * FROM ${usagestats_db}.counter_r5_stats_with_metrics; /*EOS*/