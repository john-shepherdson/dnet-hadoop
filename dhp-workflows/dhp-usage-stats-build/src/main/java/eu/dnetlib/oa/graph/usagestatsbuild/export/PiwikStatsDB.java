
package eu.dnetlib.oa.graph.usagestatsbuild.export;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class PiwikStatsDB {

	private String logPath;

	private Statement stmt = null;

	private static final Logger logger = LoggerFactory.getLogger(PiwikStatsDB.class);

	public PiwikStatsDB() throws Exception {

	}

	public void recreateDBAndTables() throws Exception {
		this.createDatabase();
		// The piwiklog table is not needed since it is built
		// on top of JSON files
		//////////// this.createTmpTables();
	}

	private void createDatabase() throws Exception {

//		try {
//
//			stmt = ConnectDB.getHiveConnection().createStatement();
//
//			logger.info("Dropping usagestats DB: " + ConnectDB.getUsageStatsDBSchema());
//			String dropDatabase = "DROP DATABASE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + " CASCADE";
//			stmt.executeUpdate(dropDatabase);
//		} catch (Exception e) {
//			logger.error("Failed to drop database: " + e);
//			throw new Exception("Failed to drop database: " + e.toString(), e);
//		}
//
		try {
			stmt = ConnectDB.getHiveConnection().createStatement();
			logger.info("Creating usagestats DB: " + ConnectDB.getUsageStatsDBSchema());
			String createDatabase = "CREATE DATABASE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema();
			stmt.executeUpdate(createDatabase);
			logger.info("Usagestats DB created: " + ConnectDB.getUsageStatsDBSchema());

		} catch (Exception e) {
			logger.error("Failed to create database: " + e);
			throw new Exception("Failed to create database: " + e.toString(), e);
		}

		try {
			stmt = ConnectDB.getHiveConnection().createStatement();

			logger.info("Creating permanent usagestats DB: " + ConnectDB.getUsagestatsPermanentDBSchema());
			String createPermanentDatabase = "CREATE DATABASE IF NOT EXISTS "
				+ ConnectDB.getUsagestatsPermanentDBSchema();
			stmt.executeUpdate(createPermanentDatabase);
			logger.info("Created permanent usagestats DB: " + ConnectDB.getUsagestatsPermanentDBSchema());

		} catch (Exception e) {
			logger.error("Failed to create database: " + e);
			throw new Exception("Failed to create database: " + e.toString(), e);
		}
	}

	public void processLogs() throws Exception {
		try {

			logger.info("ViewsStats processing starts at: " + new Timestamp(System.currentTimeMillis()));
			viewsStats();
			logger.info("ViewsStats processing ends at: " + new Timestamp(System.currentTimeMillis()));

			logger.info("DownloadsStats processing starts at: " + new Timestamp(System.currentTimeMillis()));
			downloadsStats();
			logger.info("DownloadsStats processing ends at: " + new Timestamp(System.currentTimeMillis()));

		} catch (Exception e) {
			logger.error("Failed to process logs: " + e);
			throw new Exception("Failed to process logs: " + e.toString(), e);
		}
	}

	public void viewsStats() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Dropping openaire_result_views_monthly_tmp view");
		String drop_result_views_monthly = "DROP VIEW IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_piwikresult_views_monthly_tmp";
		stmt.executeUpdate(drop_result_views_monthly);
		logger.info("Dropped openaire_result_views_monthly_tmp view");

		logger.info("Creating openaire_result_views_monthly_tmp view");
		String create_result_views_monthly = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_result_views_monthly_tmp "
			+ "AS SELECT entity_id, "
			+ "reflect('java.net.URLDecoder', 'decode', entity_id) AS id,"
			+ "COUNT(entity_id) as views, SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) "
			+ "AS openaire_referrer, "
			+ "CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source "
			+ "FROM " + ConnectDB.getUsageRawDataDBSchema()
			+ ".piwiklog where action='action' and (source_item_type='oaItem' or "
			+ "source_item_type='repItem') "
			+ "GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), "
			+ "source ORDER BY source, entity_id";
		stmt.executeUpdate(create_result_views_monthly);
		logger.info("Created openaire_result_views_monthly_tmp table");

		logger.info("Dropping openaire_views_stats_tmp table");
		String drop_views_stats = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_views_stats_tmp";
		stmt.executeUpdate(drop_views_stats);
		logger.info("Dropped openaire_views_stats_tmp table");

		logger.info("Creating openaire_views_stats_tmp table");
		String create_views_stats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_views_stats_tmp "
			+ "AS SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, "
			+ "max(views) AS count, max(openaire_referrer) AS openaire "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".openaire_result_views_monthly_tmp p, "
			+ ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro "
			+ "WHERE p.source=d.piwik_id AND p.id=ro.oid AND ro.oid!='200' "
			+ "GROUP BY d.id, ro.id, month "
			+ "ORDER BY d.id, ro.id, month ";
		stmt.executeUpdate(create_views_stats);
		logger.info("Created openaire_views_stats_tmp table");

		logger.info("Creating openaire_pageviews_stats_tmp table");
		String create_pageviews_stats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_pageviews_stats_tmp AS SELECT "
			+ "'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, max(views) AS count "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".openaire_result_views_monthly_tmp p, "
			+ ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro "
			+ "WHERE p.source=" + ExecuteWorkflow.portalMatomoID
			+ " AND p.source=d.piwik_id and p.id=ro.id AND ro.oid!='200' "
			+ "GROUP BY d.id, ro.id, month "
			+ "ORDER BY d.id, ro.id, month ";
		stmt.executeUpdate(create_pageviews_stats);
		logger.info("Created pageviews_stats table");

		stmt.close();
		// ConnectDB.getHiveConnection().close();
	}

	private void downloadsStats() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Dropping openaire_result_downloads_monthly_tmp view");
		String drop_result_downloads_monthly = "DROP VIEW IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_result_downloads_monthly_tmp";
		stmt.executeUpdate(drop_result_downloads_monthly);
		logger.info("Dropped openaire_result_downloads_monthly_tmp view");

		logger.info("Creating openaire_result_downloads_monthly_tmp view");
		String sql = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_result_downloads_monthly_tmp "
			+ "AS SELECT entity_id, "
			+ "reflect('java.net.URLDecoder', 'decode', entity_id) AS id,"
			+ "COUNT(entity_id) as downloads, "
			+ "SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer, "
			+ "CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source "
			+ "FROM " + ConnectDB.getUsageRawDataDBSchema() + ".piwiklog where action='download' "
			+ "AND (source_item_type='oaItem' OR source_item_type='repItem') "
			+ "GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) , source "
			+ "ORDER BY source, entity_id, month";
		stmt.executeUpdate(sql);
		logger.info("Created openaire_result_downloads_monthly_tmp view");

		logger.info("Dropping openaire_downloads_stats_tmp table");
		String drop_views_stats = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".openaire_downloads_stats_tmp";
		stmt.executeUpdate(drop_views_stats);
		logger.info("Dropped openaire_downloads_stats_tmp table");

		logger.info("Creating openaire_downloads_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".openaire_downloads_stats_tmp AS "
			+ "SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, "
			+ "max(downloads) AS count, max(openaire_referrer) AS openaire "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".openaire_result_downloads_monthly_tmp p, "
			+ ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro "
			+ "WHERE p.source=d.piwik_id and p.id=ro.oid AND ro.oid!='200' "
			+ "GROUP BY d.id, ro.id, month "
			+ "ORDER BY d.id, ro.id, month ";
		stmt.executeUpdate(sql);
		logger.info("Created downloads_stats table");

		logger.info("Dropping openaire_result_downloads_monthly_tmp view");
		sql = "DROP VIEW IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".openaire_result_downloads_monthly_tmp";
		logger.info("Dropped openaire_result_downloads_monthly_tmp view ");
		stmt.executeUpdate(sql);

		stmt.close();
		// ConnectDB.getHiveConnection().close();
	}

	public void uploadOldPedocs() throws Exception {
		stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		// Dropping Pedocs pedocs_views_stats_tmp table
		logger.info("Dropping Pedocs pedocs_views_stats_tmp table");
		String sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".pedocs_views_stats_tmp";
		logger.info("Dropped pedocs_views_stats_tmp table ");
		stmt.executeUpdate(sql);

		// Dropping Pedocs pedocs_downloads_stats table
		logger.info("Dropping pedocs_downloads_stats table");
		sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".pedocs_downloads_stats";
		logger.info("Dropped pedocs_downloads_stats table ");
		stmt.executeUpdate(sql);

		// Creating Pedocs pedocs_views_stats_tmp table
		logger.info("Creating Pedocs pedocs_views_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".pedocs_views_stats_tmp AS "
			+ "SELECT 'OpenAIRE' as source, 'opendoar____::ab1a4d0dd4d48a2ba1077c4494791306' as repository_id,"
			+ "r.id as result_id,date,counter_abstract as count, 0 as openaire "
			+ "FROM " + ConnectDB.getUsageRawDataDBSchema() + ".pedocsoldviews p, " + ConnectDB.getStatsDBSchema()
			+ ".result_oids r where r.oid=p.identifier";
		stmt.executeUpdate(sql);
		logger.info("Created pedocs_views_stats_tmp table ");

		// Creating Pedocs pedocs_downloads_stats_tmp table
		logger.info("Creating Pedocs pedocs_downloads_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".pedocs_downloads_stats_tmp AS "
			+ "SELECT 'OpenAIRE' as source, 'opendoar____::ab1a4d0dd4d48a2ba1077c4494791306' as repository_id,"
			+ "r.id as result_id, date, counter as count, 0 as openaire "
			+ "FROM " + ConnectDB.getUsageRawDataDBSchema() + ".pedocsolddownloads p, " + ConnectDB.getStatsDBSchema()
			+ ".result_oids r where r.oid=p.identifier";
		stmt.executeUpdate(sql);
		logger.info("Created pedocs_downloads_stats_tmp table ");

	}

	public void uploadTUDELFTStats() throws Exception {
		stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		// Dropping TUDELFT tudelft_result_views_monthly_tmp view
		logger.info("Dropping TUDELFT tudelft_result_views_monthly_tmp view");
		String sql = "DROP view IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_result_views_monthly_tmp";
		logger.info("Dropped tudelft_result_views_monthly_tmp view ");
		stmt.executeUpdate(sql);

		// Dropping TUDELFT tudelft_result_views_monthly_tmp view
		logger.info("Dropping TUDELFT tudelft_result_downloads_monthly_tmp view");
		sql = "DROP view IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_result_downloads_monthly_tmp";
		logger.info("Dropped tudelft_result_downloads_monthly_tmp view ");
		stmt.executeUpdate(sql);

		// Dropping TUDELFT tudelft_views_stats_tmp table
		logger.info("Dropping TUDELFT tudelft_views_stats_tmp table");
		sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_views_stats_tmp";
		logger.info("Dropped tudelft_views_stats_tmp table ");
		stmt.executeUpdate(sql);

		// Dropping TUDELFT tudelft_downloads_stats_tmp table
		logger.info("Dropping TUDELFT tudelft_downloads_stats_tmp table");
		sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_downloads_stats_tmp";
		logger.info("Dropped tudelft_downloads_stats_tmp table ");
		stmt.executeUpdate(sql);

		// Creating TUDELFT tudelft_result_views_monthly_tmp view
		logger.info("Creating TUDELFT tudelft_result_views_monthly_tmp view");
		sql = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_result_views_monthly_tmp "
			+ "AS SELECT entity_id, reflect('java.net.URLDecoder', 'decode', entity_id) AS id, "
			+ "COUNT(entity_id) as views, SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer, "
			+ "CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source "
			+ "FROM " + ConnectDB.getUsageRawDataDBSchema() + ".piwiklog "
			+ "WHERE action='action' and (source_item_type='oaItem' or source_item_type='repItem') and source=252 "
			+ "GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source ORDER BY source, entity_id";
		stmt.executeUpdate(sql);
		logger.info("Created tudelft_result_views_monthly_tmp view ");

		// Creating TUDELFT tudelft_views_stats_tmp table
		logger.info("Creating TUDELFT tudelft_views_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_views_stats_tmp AS "
			+ "SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, "
			+ "max(views) AS count, max(openaire_referrer) AS openaire FROM " + ConnectDB.getUsageStatsDBSchema()
			+ ".tudelft_result_views_monthly_tmp p, "
			+ ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro "
			+ "WHERE concat('tud:',p.id)=ro.oid and d.id='opendoar____::c9892a989183de32e976c6f04e700201' "
			+ "GROUP BY d.id, ro.id, month ORDER BY d.id, ro.id";
		stmt.executeUpdate(sql);
		logger.info("Created TUDELFT tudelft_views_stats_tmp table");

		// Creating TUDELFT tudelft_result_downloads_monthly_tmp view
		logger.info("Creating TUDELFT tudelft_result_downloads_monthly_tmp view");
		sql = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_result_downloads_monthly_tmp "
			+ "AS SELECT entity_id, reflect('java.net.URLDecoder', 'decode', entity_id) AS id, "
			+ "COUNT(entity_id) as views, SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer, "
			+ "CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source "
			+ "FROM " + ConnectDB.getUsageRawDataDBSchema() + ".piwiklog "
			+ "WHERE action='download' and (source_item_type='oaItem' or source_item_type='repItem') and source=252 "
			+ "GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), source ORDER BY source, entity_id";
		stmt.executeUpdate(sql);
		logger.info("Created tudelft_result_downloads_monthly_tmp view ");

		// Creating TUDELFT tudelft_downloads_stats_tmp table
		logger.info("Creating TUDELFT tudelft_downloads_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_downloads_stats_tmp AS "
			+ "SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, "
			+ "max(views) AS count, max(openaire_referrer) AS openaire FROM " + ConnectDB.getUsageStatsDBSchema()
			+ ".tudelft_result_downloads_monthly_tmp p, "
			+ ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro "
			+ "WHERE concat('tud:',p.id)=ro.oid and d.id='opendoar____::c9892a989183de32e976c6f04e700201' "
			+ "GROUP BY d.id, ro.id, month ORDER BY d.id, ro.id";
		stmt.executeUpdate(sql);
		logger.info("Created TUDELFT tudelft_downloads_stats_tmp table");

		// Dropping TUDELFT tudelft_result_views_monthly_tmp view
		logger.info("Dropping TUDELFT tudelft_result_views_monthly_tmp view");
		sql = "DROP view IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_result_views_monthly_tmp";
		logger.info("Dropped tudelft_result_views_monthly_tmp view ");
		stmt.executeUpdate(sql);

		// Dropping TUDELFT tudelft_result_views_monthly_tmp view
		logger.info("Dropping TUDELFT tudelft_result_downloads_monthly_tmp view");
		sql = "DROP view IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_result_downloads_monthly_tmp";
		logger.info("Dropped tudelft_result_downloads_monthly_tmp view ");
		stmt.executeUpdate(sql);

	}

	public void finalizeStats() throws Exception {
		stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		// Dropping views_stats table
		logger.info("Dropping views_stats table");
		String sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".views_stats";
		logger.info("Dropped views_stats table ");
		stmt.executeUpdate(sql);

		// Dropping downloads_stats table
		logger.info("Dropping downloads_stats table");
		sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats";
		logger.info("Dropped downloads_stats table ");
		stmt.executeUpdate(sql);

		// Dropping page_views_stats table
		logger.info("Dropping pageviews_stats table");
		sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats";
		logger.info("Dropped pageviews_stats table ");
		stmt.executeUpdate(sql);

		// Dropping usage_stats table
		logger.info("Dropping usage_stats table");
		sql = "DROP TABLE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".usage_stats";
		logger.info("Dropped usage_stats table ");
		stmt.executeUpdate(sql);

		// Creating views_stats table
		logger.info("Creating views_stats table");
		String createViewsStats = "CREATE TABLE IF NOT EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".views_stats "
			+ "LIKE " + ConnectDB.getUsageStatsDBSchema() + ".openaire_views_stats_tmp STORED AS PARQUET";
		stmt.executeUpdate(createViewsStats);
		logger.info("Created views_stats table");

		// Inserting OpenAIRE views stats
		logger.info("Inserting Openaire data to views_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".views_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".openaire_views_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Openaire views updated to views_stats");

		// Inserting Pedocs old views stats
		logger.info("Inserting Pedocs old data to views_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".views_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".pedocs_views_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Pedocs views updated to views_stats");

		// Inserting TUDELFT views stats
		logger.info("Inserting TUDELFT data to views_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".views_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_views_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("TUDELFT views updated to views_stats");

		// Inserting Lareferencia views stats
		logger.info("Inserting LaReferencia data to views_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".views_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".la_views_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("LaReferencia views updated to views_stats");

		logger.info("Creating downloads_stats table");
		String createDownloadsStats = "CREATE TABLE IF NOT EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".downloads_stats "
			+ "LIKE " + ConnectDB.getUsageStatsDBSchema() + ".openaire_downloads_stats_tmp STORED AS PARQUET";
		stmt.executeUpdate(createDownloadsStats);
		logger.info("Created downloads_stats table");

		// Inserting OpenAIRE downloads stats
		logger.info("Inserting OpenAIRE data to downloads_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".openaire_downloads_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Inserted OpenAIRE data to downloads_stats");

		// Inserting Pedocs old downloads stats
		logger.info("Inserting PeDocs old data to downloads_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".pedocs_downloads_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Inserted Pedocs data to downloads_stats");

		// Inserting TUDELFT downloads stats
		logger.info("Inserting TUDELFT old data to downloads_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".tudelft_downloads_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Inserted TUDELFT data to downloads_stats");

		// Inserting Lareferencia downloads stats
		logger.info("Inserting LaReferencia data to downloads_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".la_downloads_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("Lareferencia downloads updated to downloads_stats");

		// Inserting IRUS downloads stats
		logger.info("Inserting IRUS data to downloads_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".irus_downloads_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("IRUS downloads updated to downloads_stats");

		// Inserting SARC-OJS downloads stats
		logger.info("Inserting SARC data to downloads_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".sarc_downloads_stats_tmp";
		stmt.executeUpdate(sql);
		logger.info("SARC-OJS downloads updated to downloads_stats");

		logger.info("Creating pageviews_stats table");
		String create_pageviews_stats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".pageviews_stats "
			+ "LIKE " + ConnectDB.getUsageStatsDBSchema() + ".openaire_pageviews_stats_tmp STORED AS PARQUET";
		stmt.executeUpdate(create_pageviews_stats);
		logger.info("Created pageviews_stats table");

		// Inserting OpenAIRE views stats from Portal
		logger.info("Inserting data to page_views_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".openaire_pageviews_stats_tmp";
		stmt.executeUpdate(sql);

		logger.info("Dropping full_dates table");
		String dropFullDates = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".full_dates";
		stmt.executeUpdate(dropFullDates);
		logger.info("Dropped full_dates table");

		Calendar startCalendar = Calendar.getInstance();
		startCalendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01"));
		Calendar endCalendar = Calendar.getInstance();
		int diffYear = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
		int diffMonth = diffYear * 12 + endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);

		logger.info("Creating full_dates table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".full_dates AS "
			+ "SELECT from_unixtime(unix_timestamp(cast(add_months(from_date,i) AS DATE)), 'yyyy/MM') AS txn_date "
			+ "FROM (SELECT DATE '2016-01-01' AS from_date) p "
			+ "LATERAL VIEW "
			+ "posexplode(split(space(" + diffMonth + "),' ')) pe AS i,x";
		stmt.executeUpdate(sql);
		logger.info("Created full_dates table");

		logger.info("Inserting data to usage_stats");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".usage_stats AS "
			+ "SELECT coalesce(ds.source, vs.source) as source, "
			+ "coalesce(ds.repository_id, vs.repository_id) as repository_id, "
			+ "coalesce(ds.result_id, vs.result_id) as result_id, coalesce(ds.date, vs.date) as date, "
			+ "coalesce(ds.count, 0) as downloads, coalesce(vs.count, 0) as views, "
			+ "coalesce(ds.openaire, 0) as openaire_downloads, "
			+ "coalesce(vs.openaire, 0) as openaire_views "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats AS ds FULL OUTER JOIN "
			+ ConnectDB.getUsageStatsDBSchema() + ".views_stats AS vs ON ds.source=vs.source "
			+ "AND ds.repository_id=vs.repository_id AND ds.result_id=vs.result_id AND ds.date=vs.date";
		stmt.executeUpdate(sql);
		logger.info("Inserted data to usage_stats");

		logger.info("Building views at permanent DB starts at: " + new Timestamp(System.currentTimeMillis()));

		logger.info("Dropping view views_stats on permanent usagestats DB");
		sql = "DROP VIEW IF EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".views_stats";
		stmt.executeUpdate(sql);
		logger.info("Dropped view views_stats on permanent usagestats DB");

		logger.info("Create view views_stats on permanent usagestats DB");
		sql = "CREATE VIEW IF NOT EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".views_stats"
			+ " AS SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".views_stats";
		stmt.executeUpdate(sql);
		logger.info("Created view views_stats on permanent usagestats DB");

		logger.info("Dropping view pageviews_stats on permanent usagestats DB");
		sql = "DROP VIEW IF EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".pageviews_stats";
		stmt.executeUpdate(sql);
		logger.info("Dropped view pageviews_stats on permanent usagestats DB");

		logger.info("Create view pageviews_stats on permanent usagestats DB");
		sql = "CREATE VIEW IF NOT EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".pageviews_stats"
			+ " AS SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats";
		stmt.executeUpdate(sql);
		logger.info("Created view pageviews_stats on permanent usagestats DB");

		logger.info("Dropping view downloads_stats on permanent usagestats DB");
		sql = "DROP VIEW IF EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".downloads_stats";
		stmt.executeUpdate(sql);
		logger.info("Dropped view on downloads_stats on permanent usagestats DB");

		logger.info("Create view on downloads_stats on permanent usagestats DB");
		sql = "CREATE VIEW IF NOT EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".downloads_stats"
			+ " AS SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats";
		stmt.executeUpdate(sql);
		logger.info("Created view on downloads_stats on permanent usagestats DB");

		logger.info("Dropping view usage_stats on permanent usagestats DB");
		sql = "DROP VIEW IF EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".usage_stats";
		stmt.executeUpdate(sql);
		logger.info("Dropped view on usage_stats on permanent usagestats DB");

		logger.info("Create view on usage_stats on permanent usagestats DB");
		sql = "CREATE VIEW IF NOT EXISTS " + ConnectDB.getUsagestatsPermanentDBSchema() + ".usage_stats"
			+ " AS SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".usage_stats";
		stmt.executeUpdate(sql);
		logger.info("Created view on usage_stats on permanent usagestats DB");

		logger.info("Building views at permanent DB ends at: " + new Timestamp(System.currentTimeMillis()));

		stmt.close();
		ConnectDB.getHiveConnection().close();
	}

	private Connection getConnection() throws SQLException {
		return ConnectDB.getHiveConnection();
	}
}
