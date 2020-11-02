
package eu.dnetlib.oa.graph.usagestats.export;

import java.io.*;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class PiwikStatsDB {

	private String logPath;
	private String logRepoPath;
	private String logPortalPath;

	private Statement stmt = null;

	private static final Logger logger = LoggerFactory.getLogger(PiwikStatsDB.class);

	private String CounterRobotsURL;
	private ArrayList robotsList;

	public PiwikStatsDB(String logRepoPath, String logPortalPath) throws Exception {
		this.logRepoPath = logRepoPath;
		this.logPortalPath = logPortalPath;

	}

	public void reCreateLogDirs() throws IllegalArgumentException, IOException {
		FileSystem dfs = FileSystem.get(new Configuration());

		logger.info("Deleting repoLog directory: " + ExecuteWorkflow.repoLogPath);
		dfs.delete(new Path(ExecuteWorkflow.repoLogPath), true);

		logger.info("Deleting portalLog directory: " + ExecuteWorkflow.portalLogPath);
		dfs.delete(new Path(ExecuteWorkflow.portalLogPath), true);

		logger.info("Creating repoLog directory: " + ExecuteWorkflow.repoLogPath);
		dfs.mkdirs(new Path(ExecuteWorkflow.repoLogPath));

		logger.info("Creating portalLog directory: " + ExecuteWorkflow.portalLogPath);
		dfs.mkdirs(new Path(ExecuteWorkflow.portalLogPath));
	}

	public void recreateDBAndTables() throws Exception {
		this.createDatabase();
		this.createTables();
		// The piwiklog table is not needed since it is built
		// on top of JSON files
		this.createTmpTables();
	}

	public ArrayList getRobotsList() {
		return robotsList;
	}

	public void setRobotsList(ArrayList robotsList) {
		this.robotsList = robotsList;
	}

	public String getCounterRobotsURL() {
		return CounterRobotsURL;
	}

	public void setCounterRobotsURL(String CounterRobotsURL) {
		this.CounterRobotsURL = CounterRobotsURL;
	}

	private void createDatabase() throws Exception {
		try {
			stmt = ConnectDB.getHiveConnection().createStatement();

			logger.info("Dropping usagestats DB: " + ConnectDB.getUsageStatsDBSchema());
			String dropDatabase = "DROP DATABASE IF EXISTS " + ConnectDB.getUsageStatsDBSchema() + " CASCADE";
			stmt.executeUpdate(dropDatabase);
		} catch (Exception e) {
			logger.error("Failed to drop database: " + e);
			throw new Exception("Failed to drop database: " + e.toString(), e);
		}

		try {
			stmt = ConnectDB.getHiveConnection().createStatement();

			logger.info("Creating usagestats DB: " + ConnectDB.getUsageStatsDBSchema());
			String createDatabase = "CREATE DATABASE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema();
			stmt.executeUpdate(createDatabase);

		} catch (Exception e) {
			logger.error("Failed to create database: " + e);
			throw new Exception("Failed to create database: " + e.toString(), e);
		}
	}

	private void createTables() throws Exception {
		try {
			stmt = ConnectDB.getHiveConnection().createStatement();

			// Create Piwiklog table - This table should exist
			String sqlCreateTablePiwikLog = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getUsageStatsDBSchema()
				+ ".piwiklog(source INT, id_visit STRING, country STRING, action STRING, url STRING, "
				+ "entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) "
				+ "clustered by (source, id_visit, action, timestamp, entity_id) "
				+ "into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTablePiwikLog);

			/////////////////////////////////////////
			// Rule for duplicate inserts @ piwiklog
			/////////////////////////////////////////

			String sqlCreateTablePortalLog = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getUsageStatsDBSchema()
				+ ".process_portal_log(source INT, id_visit STRING, country STRING, action STRING, url STRING, "
				+ "entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) "
				+ "clustered by (source, id_visit, timestamp) into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTablePortalLog);

			//////////////////////////////////////////////////
			// Rule for duplicate inserts @ process_portal_log
			//////////////////////////////////////////////////

			stmt.close();
			ConnectDB.getHiveConnection().close();

		} catch (Exception e) {
			logger.error("Failed to create tables: " + e);
			throw new Exception("Failed to create tables: " + e.toString(), e);
		}
	}

	private void createTmpTables() throws Exception {
		try {
			Statement stmt = ConnectDB.getHiveConnection().createStatement();
			String sqlCreateTmpTablePiwikLog = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getUsageStatsDBSchema()
				+ ".piwiklogtmp(source INT, id_visit STRING, country STRING, action STRING, url STRING, entity_id STRING, "
				+ "source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) "
				+ "clustered by (source, id_visit, action, timestamp, entity_id) into 100 buckets "
				+ "stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTmpTablePiwikLog);

			//////////////////////////////////////////////////
			// Rule for duplicate inserts @ piwiklogtmp
			//////////////////////////////////////////////////

			//////////////////////////////////////////////////
			// Copy from public.piwiklog to piwiklog
			//////////////////////////////////////////////////
			// String sqlCopyPublicPiwiklog="insert into piwiklog select * from public.piwiklog;";
			// stmt.executeUpdate(sqlCopyPublicPiwiklog);

			String sqlCreateTmpTablePortalLog = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getUsageStatsDBSchema()
				+ ".process_portal_log_tmp(source INT, id_visit STRING, country STRING, action STRING, url STRING, "
				+ "entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) "
				+ "clustered by (source, id_visit, timestamp) into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTmpTablePortalLog);

			//////////////////////////////////////////////////
			// Rule for duplicate inserts @ process_portal_log_tmp
			//////////////////////////////////////////////////

			stmt.close();

		} catch (Exception e) {
			logger.error("Failed to create tmptables: " + e);
			throw new Exception("Failed to create tmp tables: " + e.toString(), e);
			// System.exit(0);
		}
	}

	public void processLogs() throws Exception {
		try {
			ReadCounterRobotsList counterRobots = new ReadCounterRobotsList(this.getCounterRobotsURL());
			this.robotsList = counterRobots.getRobotsPatterns();

			logger.info("Processing repository logs");
			processRepositoryLog();
			logger.info("Repository logs process done");

			logger.info("Removing double clicks");
			removeDoubleClicks();
			logger.info("Removing double clicks done");

			logger.info("Cleaning oai");
			cleanOAI();
			logger.info("Cleaning oai done");

			logger.info("Processing portal logs");
			processPortalLog();
			logger.info("Portal logs process done");

			logger.info("Processing portal usagestats");
			portalStats();
			logger.info("Portal usagestats process done");

			logger.info("ViewsStats processing starts");
			viewsStats();
			logger.info("ViewsStats processing ends");

			logger.info("DownloadsStats processing starts");
			downloadsStats();
			logger.info("DownloadsStats processing starts");

			logger.info("Updating Production Tables");
			updateProdTables();
			logger.info("Updated Production Tables");

		} catch (Exception e) {
			logger.error("Failed to process logs: " + e);
			throw new Exception("Failed to process logs: " + e.toString(), e);
		}
	}

	public void processRepositoryLog() throws Exception {

		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Adding JSON Serde jar");
		stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
		logger.info("Added JSON Serde jar");

		logger.info("Dropping piwiklogtmp_json table");
		String drop_piwiklogtmp_json = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".piwiklogtmp_json";
		stmt.executeUpdate(drop_piwiklogtmp_json);
		logger.info("Dropped piwiklogtmp_json table");

		logger.info("Creating piwiklogtmp_json");
		String create_piwiklogtmp_json = "CREATE EXTERNAL TABLE IF NOT EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".piwiklogtmp_json(\n" +
			"	`idSite` STRING,\n" +
			"	`idVisit` STRING,\n" +
			"	`country` STRING,\n" +
			"	`referrerName` STRING,\n" +
			"	`browser` STRING,\n" +
			"	`actionDetails` ARRAY<\n" +
			"						struct<\n" +
			"							type: STRING,\n" +
			"							url: STRING,\n" +
			"							`customVariables`: struct<\n" +
			"								`1`: struct<\n" +
			"								`customVariablePageValue1`: STRING\n" +
			"										>\n" +
			"								>,\n" +
			"							timestamp: String\n" +
			"							>\n" +
			"						>\n" +
			")\n" +
			"ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
			"LOCATION '" + ExecuteWorkflow.repoLogPath + "'\n" +
			"TBLPROPERTIES (\"transactional\"=\"false\")";
		stmt.executeUpdate(create_piwiklogtmp_json);
		logger.info("Created piwiklogtmp_json");

		logger.info("Dropping piwiklogtmp table");
		String drop_piwiklogtmp = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".piwiklogtmp";
		stmt.executeUpdate(drop_piwiklogtmp);
		logger.info("Dropped piwiklogtmp");

		logger.info("Creating piwiklogtmp");
		String create_piwiklogtmp = "CREATE TABLE " +
			ConnectDB.getUsageStatsDBSchema() +
			".piwiklogtmp (source BIGINT, id_Visit STRING, country STRING, action STRING, url STRING, " +
			"entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING)  " +
			"clustered by (source) into 100 buckets stored as orc tblproperties('transactional'='true')";
		stmt.executeUpdate(create_piwiklogtmp);
		logger.info("Created piwiklogtmp");

		logger.info("Inserting into piwiklogtmp");
		String insert_piwiklogtmp = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SELECT DISTINCT cast(idSite as BIGINT) as source, idVisit  as id_Visit, country, " +
			"actiondetail.type as action, actiondetail.url as url, " +
			"actiondetail.customVariables.`1`.`customVariablePageValue1` as entity_id, " +
			"'repItem' as source_item_type, from_unixtime(cast(actiondetail.timestamp as BIGINT)) as timestamp, " +
			"referrerName as referrer_name, browser as agent\n" +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp_json\n" +
			"LATERAL VIEW explode(actiondetails) actiondetailsTable AS actiondetail";
		stmt.executeUpdate(insert_piwiklogtmp);
		logger.info("Inserted into piwiklogtmp");

		stmt.close();
	}

	public void removeDoubleClicks() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Cleaning download double clicks");
		// clean download double clicks
		String sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"WHERE EXISTS (\n" +
			"SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp \n" +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p1, " +
			ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p2\n" +
			"WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id \n"
			+
			"AND p1.action=p2.action AND p1.action='download' AND p1.timestamp!=p2.timestamp \n" +
			"AND p1.timestamp<p2.timestamp AND ((unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))/60)<30 \n" +
			"AND piwiklogtmp.source=p1.source AND piwiklogtmp.id_visit=p1.id_visit \n" +
			"AND piwiklogtmp.action=p1.action AND piwiklogtmp.entity_id=p1.entity_id AND piwiklogtmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		logger.info("Cleaned download double clicks");

		// clean view double clicks
		logger.info("Cleaning action double clicks");
		sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"WHERE EXISTS (\n" +
			"SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp \n" +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p1, " +
			ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p2\n" +
			"WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id \n"
			+
			"AND p1.action=p2.action AND p1.action='action' AND p1.timestamp!=p2.timestamp \n" +
			"AND p1.timestamp<p2.timestamp AND (unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))<10 \n" +
			"AND piwiklogtmp.source=p1.source AND piwiklogtmp.id_visit=p1.id_visit \n" +
			"AND piwiklogtmp.action=p1.action AND piwiklogtmp.entity_id=p1.entity_id AND piwiklogtmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		logger.info("Cleaned action double clicks");
		stmt.close();
	}

	public void viewsStats() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Dropping result_views_monthly_tmp table");
		String drop_result_views_monthly_tmp = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".result_views_monthly_tmp";
		stmt.executeUpdate(drop_result_views_monthly_tmp);
		logger.info("Dropped result_views_monthly_tmp table");

		logger.info("Creating result_views_monthly_tmp table");
		String create_result_views_monthly_tmp = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema()
			+ ".result_views_monthly_tmp " +
			"AS SELECT entity_id AS id, " +
			"COUNT(entity_id) as views, SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) " +
			"AS openaire_referrer, " +
			"CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source " +
			"FROM " + ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklogtmp where action='action' and (source_item_type='oaItem' or " +
			"source_item_type='repItem') " +
			"GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')), " +
			"source ORDER BY source, entity_id";
		stmt.executeUpdate(create_result_views_monthly_tmp);
		logger.info("Created result_views_monthly_tmp table");

		logger.info("Dropping views_stats_tmp table");
		String drop_views_stats_tmp = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".views_stats_tmp";
		stmt.executeUpdate(drop_views_stats_tmp);
		logger.info("Dropped views_stats_tmp table");

		logger.info("Creating views_stats_tmp table");
		String create_views_stats_tmp = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".views_stats_tmp " +
			"AS SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, " +
			"max(views) AS count, max(openaire_referrer) AS openaire " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".result_views_monthly_tmp p, " +
			ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro " +
			"WHERE p.source=d.piwik_id AND p.id=ro.oid " +
			"GROUP BY d.id, ro.id, month " +
			"ORDER BY d.id, ro.id, month";
		stmt.executeUpdate(create_views_stats_tmp);
		logger.info("Created views_stats_tmp table");
/*
		logger.info("Dropping views_stats table");
		String drop_views_stats = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".views_stats";
		stmt.executeUpdate(drop_views_stats);
		logger.info("Dropped views_stats table");
*/
		logger.info("Creating views_stats table");
		String create_view_stats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".views_stats " +
			"LIKE " + ConnectDB.getUsageStatsDBSchema() + ".views_stats_tmp STORED AS PARQUET";
		stmt.executeUpdate(create_view_stats);
		logger.info("Created views_stats table");

		logger.info("Dropping pageviews_stats_tmp table");
		String drop_pageviews_stats_tmp = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".pageviews_stats_tmp";
		stmt.executeUpdate(drop_pageviews_stats_tmp);
		logger.info("Dropped pageviews_stats_tmp table");

		logger.info("Creating pageviews_stats_tmp table");
		String create_pageviews_stats_tmp = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".pageviews_stats_tmp AS SELECT " +
			"'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, max(views) AS count " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".result_views_monthly_tmp p, " +
			ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro " +
			"WHERE p.source=" + ExecuteWorkflow.portalMatomoID + " AND p.source=d.piwik_id and p.id=ro.id \n" +
			"GROUP BY d.id, ro.id, month " +
			"ORDER BY d.id, ro.id, month";
		stmt.executeUpdate(create_pageviews_stats_tmp);
		logger.info("Created pageviews_stats_tmp table");

/*		logger.info("Droping pageviews_stats table");
		String drop_pageviews_stats = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".pageviews_stats";
		stmt.executeUpdate(drop_pageviews_stats);
		logger.info("Dropped pageviews_stats table");
*/
		logger.info("Creating pageviews_stats table");
		String create_pageviews_stats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".pageviews_stats " +
			"LIKE " + ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats_tmp STORED AS PARQUET";
		stmt.executeUpdate(create_pageviews_stats);
		logger.info("Created pageviews_stats table");

		stmt.close();
		ConnectDB.getHiveConnection().close();
	}

	private void downloadsStats() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Dropping result_downloads_monthly_tmp view");
		String drop_result_downloads_monthly_tmp = "DROP VIEW IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".result_downloads_monthly_tmp";
		stmt.executeUpdate(drop_result_downloads_monthly_tmp);
		logger.info("Dropped result_downloads_monthly_tmp view");

		logger.info("Creating result_downloads_monthly_tmp view");
		String sql = "CREATE OR REPLACE VIEW " + ConnectDB.getUsageStatsDBSchema() + ".result_downloads_monthly_tmp " +
			"AS SELECT entity_id AS id, COUNT(entity_id) as downloads, " +
			"SUM(CASE WHEN referrer_name LIKE '%openaire%' THEN 1 ELSE 0 END) AS openaire_referrer, " +
			"CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) AS month, source " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp where action='download' " +
			"AND (source_item_type='oaItem' OR source_item_type='repItem') " +
			"GROUP BY entity_id, CONCAT(YEAR(timestamp), '/', LPAD(MONTH(timestamp), 2, '0')) , source " +
			"ORDER BY source, entity_id, month";
		stmt.executeUpdate(sql);
		logger.info("Created result_downloads_monthly_tmp view");

		logger.info("Dropping downloads_stats_tmp table");
		String drop_views_stats = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".downloads_stats_tmp";
		stmt.executeUpdate(drop_views_stats);
		logger.info("Dropped downloads_stats_tmp table");

		logger.info("Creating downloads_stats_tmp table");
		sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats_tmp AS " +
			"SELECT 'OpenAIRE' as source, d.id as repository_id, ro.id as result_id, month as date, " +
			"max(downloads) AS count, max(openaire_referrer) AS openaire " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".result_downloads_monthly_tmp p, " +
			ConnectDB.getStatsDBSchema() + ".datasource d, " + ConnectDB.getStatsDBSchema() + ".result_oids ro " +
			"WHERE p.source=d.piwik_id and p.id=ro.oid " +
			"GROUP BY d.id, ro.id, month " +
			"ORDER BY d.id, ro.id, month";
		stmt.executeUpdate(sql);
		logger.info("Created downloads_stats_tmp table");
/*
		logger.info("Dropping downloads_stats table");
		String drop_downloads_stats = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".downloads_stats";
		stmt.executeUpdate(drop_downloads_stats);
		logger.info("Dropped downloads_stats table");
*/
		logger.info("Creating downloads_stats table");
		String create_downloads_stats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".downloads_stats " +
			"LIKE " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats_tmp STORED AS PARQUET ";
		stmt.executeUpdate(create_downloads_stats);
		logger.info("Created downloads_stats table");

		logger.info("Dropping result_downloads_monthly_tmp view");
		sql = "DROP VIEW IF EXISTS result_downloads_monthly_tmp";
		logger.info("Dropped result_downloads_monthly_tmp view");
		stmt.executeUpdate(sql);

		stmt.close();
		ConnectDB.getHiveConnection().close();
	}

	public void finalizeStats() throws Exception {
		stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Dropping full_dates table");
		String dropFullDates = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".full_dates";
		stmt.executeUpdate(dropFullDates);
		logger.info("Dropped full_dates table");

		Calendar startCalendar = Calendar.getInstance();
		startCalendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01"));
		Calendar endCalendar = Calendar.getInstance();
		int diffYear = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
		int diffMonth = diffYear * 12 + endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);

		logger.info("Creating full_dates table");
		String sql = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".full_dates AS " +
			"SELECT from_unixtime(unix_timestamp(cast(add_months(from_date,i) AS DATE)), 'yyyy/MM') AS txn_date " +
			"FROM (SELECT DATE '2016-01-01' AS from_date) p " +
			"LATERAL VIEW " +
			"posexplode(split(space(" + diffMonth + "),' ')) pe AS i,x";
		stmt.executeUpdate(sql);
		logger.info("Created full_dates table");

		logger.info("Creating downloads_stats table");
		String createDownloadsStats = "CREATE TABLE IF NOT EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".downloads_stats " +
			"(`source` string, " +
			"`repository_id` string, " +
			"`result_id` string, " +
			"`date` string, " +
			"`count` bigint, " +
			"`openaire` bigint)";
		stmt.executeUpdate(createDownloadsStats);
		logger.info("Created downloads_stats table");

		logger.info("Creating views_stats table");
		String createViewsStats = "CREATE TABLE IF NOT EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".views_stats " +
			"(`source` string, " +
			"`repository_id` string, " +
			"`result_id` string, " +
			"`date` string, " +
			"`count` bigint, " +
			"`openaire` bigint)";
		stmt.executeUpdate(createViewsStats);
		logger.info("Created views_stats table");

		String createUsageStats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema() + ".usage_stats " +
			"AS SELECT coalesce(ds.source, vs.source) as source, " +
			"coalesce(ds.repository_id, vs.repository_id) as repository_id, " +
			"coalesce(ds.result_id, vs.result_id) as result_id, coalesce(ds.date, vs.date) as date, " +
			"coalesce(ds.count, 0) as downloads, coalesce(vs.count, 0) as views, " +
			"coalesce(ds.openaire, 0) as openaire_downloads, " +
			"coalesce(vs.openaire, 0) as openaire_views " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats AS ds FULL OUTER JOIN " +
			ConnectDB.getUsageStatsDBSchema() + ".views_stats AS vs ON ds.source=vs.source " +
			"AND ds.repository_id=vs.repository_id AND ds.result_id=vs.result_id AND ds.date=vs.date";
		stmt.executeUpdate(createUsageStats);

		stmt.close();
		ConnectDB.getHiveConnection().close();
	}

	// Create repository Views statistics
	private void repositoryViewsStats() throws Exception {
		stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

//        String sql = "SELECT entity_id AS id , COUNT(entity_id) AS number_of_views, timestamp::date AS date, source INTO repo_view_stats FROM piwiklog WHERE source!='5' AND action=\'action\' AND source_item_type=\'repItem\' GROUP BY entity_id, date, source ORDER BY entity_id, date ASC, COUNT(entity_id) DESC;";
		String sql = "CREATE TABLE IF NOT EXISTS repo_view_stats AS SELECT entity_id AS id , COUNT(entity_id) AS number_of_views, timestamp::date AS date, source FROM piwiklog WHERE source!='5' AND action=\'action\' AND source_item_type=\'repItem\' GROUP BY entity_id, date, source ORDER BY entity_id, date ASC, COUNT(entity_id) DESC;";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_id ON repo_view_stats USING btree (id)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_date ON repo_view_stats USING btree(date)";
		stmt.executeUpdate(sql);

//        sql = "SELECT roid.id, sum(number_of_views), extract('year' from date) ||'/'|| LPAD(CAST(extract('month' from date) AS VARCHAR), 2, '0') AS month, source INTO repo_view_stats_monthly_clean FROM repo_view_stats rvs, result_oids roid where rvs.id=roid.orid group by roid.id, month, source;";
		sql = "CREATE TABLE IF NOT EXISTS repo_view_stats_monthly_clean AS SELECT roid.id, sum(number_of_views), extract('year' from date) ||'/'|| LPAD(CAST(extract('month' from date) AS VARCHAR), 2, '0') AS month, source FROM repo_view_stats rvs, result_oids roid where rvs.id=roid.orid group by roid.id, month, source;";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_monthly_clean_id ON repo_view_stats_monthly_clean USING btree (id)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_monthly_clean_month ON repo_view_stats_monthly_clean USING btree(month)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_monthly_clean_source ON repo_view_stats_monthly_clean USING btree(source)";
		stmt.executeUpdate(sql);

		Calendar startCalendar = Calendar.getInstance();
		startCalendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01"));
		Calendar endCalendar = Calendar.getInstance();
		int diffYear = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
		int diffMonth = diffYear * 12 + endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);

		// sql="CREATE OR REPLACE view repo_view_stats_monthly AS select d.id, d.new_date AS month, case when rdm.sum is
		// null then 0 else rdm.sum end, d.source from (select distinct rdsm.id, to_char(date_trunc('month',
		// ('2016-01-01'::date + interval '1 month'*offs)), 'YYYY/MM') AS new_date, rdsm.source from generate_series(0,
		// " + diffMonth +", 1) AS offs, repo_view_stats_monthly_clean rdsm) d LEFT JOIN (select id, month, sum, source
		// from repo_view_stats_monthly_clean) rdm ON d.new_date=rdm.month and d.id=rdm.id and d.source=rdm.source order
		// by d.id, d.new_date";
//        sql = "select d.id, d.new_date AS month, case when rdm.sum is null then 0 else rdm.sum end, d.source INTO repo_view_stats_monthly from (select distinct rdsm.id, to_char(date_trunc('month', ('2016-01-01'::date + interval '1 month'*offs)), 'YYYY/MM') AS new_date, rdsm.source from generate_series(0, " + diffMonth + ", 1) AS offs, repo_view_stats_monthly_clean rdsm) d LEFT JOIN (select id, month, sum, source from repo_view_stats_monthly_clean) rdm ON d.new_date=rdm.month and d.id=rdm.id and d.source=rdm.source order by d.id, d.new_date";
		sql = "CREATE TABLE IF NOT EXISTS repo_view_stats_monthly AS select d.id, d.new_date AS month, case when rdm.sum is null then 0 else rdm.sum end, d.source from (select distinct rdsm.id, to_char(date_trunc('month', ('2016-01-01'::date + interval '1 month'*offs)), 'YYYY/MM') AS new_date, rdsm.source from generate_series(0, "
			+ diffMonth
			+ ", 1) AS offs, repo_view_stats_monthly_clean rdsm) d LEFT JOIN (select id, month, sum, source from repo_view_stats_monthly_clean) rdm ON d.new_date=rdm.month and d.id=rdm.id and d.source=rdm.source order by d.id, d.new_date";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_monthly_id ON repo_view_stats_monthly USING btree (id)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_monthly_month ON repo_view_stats_monthly USING btree(month)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_view_stats_monthly_source ON repo_view_stats_monthly USING btree(source)";
		stmt.executeUpdate(sql);

		sql = "CREATE OR REPLACE view repo_view_stats_monthly_sushi AS SELECT id, sum(number_of_views), extract('year' from date) ||'-'|| LPAD(CAST(extract('month' from date) AS VARCHAR), 2, '0') ||'-01' AS month, source FROM repo_view_stats group by id, month, source;";
		stmt.executeUpdate(sql);

		stmt.close();
		ConnectDB.getHiveConnection().commit();
		ConnectDB.getHiveConnection().close();
	}

	// Create repository downloads statistics
	private void repositoryDownloadsStats() throws Exception {
		stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

//        String sql = "SELECT entity_id AS id, COUNT(entity_id) AS number_of_downloads, timestamp::date AS date, source INTO repo_download_stats FROM piwiklog WHERE source!='5' AND action=\'download\' AND source_item_type=\'repItem\' GROUP BY entity_id, date, source ORDER BY entity_id, date ASC, COUNT(entity_id) DESC;";
		String sql = "CREATE TABLE IF NOT EXISTS repo_download_stats AS SELECT entity_id AS id, COUNT(entity_id) AS number_of_downloads, timestamp::date AS date, source FROM piwiklog WHERE source!='5' AND action=\'download\' AND source_item_type=\'repItem\' GROUP BY entity_id, date, source ORDER BY entity_id, date ASC, COUNT(entity_id) DESC;";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_id ON repo_download_stats USING btree (id)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_date ON repo_download_stats USING btree(date)";
		stmt.executeUpdate(sql);

//        sql = "SELECT roid.id, sum(number_of_downloads), extract('year' from date) ||'/'|| LPAD(CAST(extract('month' from date) AS VARCHAR), 2, '0') AS month, source INTO repo_download_stats_monthly_clean FROM repo_download_stats rvs, result_oids roid WHERE rvs.id=roid.orid GROUP BY roid.id, month, source;";
		sql = "CREATE TABLE IF NOT EXISTS repo_download_stats_monthly_clean AS SELECT roid.id, sum(number_of_downloads), extract('year' from date) ||'/'|| LPAD(CAST(extract('month' from date) AS VARCHAR), 2, '0') AS month, source FROM repo_download_stats rvs, result_oids roid WHERE rvs.id=roid.orid GROUP BY roid.id, month, source;";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_monthly_clean_id ON repo_download_stats_monthly_clean USING btree (id)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_monthly_clean_month ON repo_download_stats_monthly_clean USING btree(month)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_monthly_clean_source ON repo_download_stats_monthly_clean USING btree(source)";
		stmt.executeUpdate(sql);

		Calendar startCalendar = Calendar.getInstance();
		startCalendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01"));
		Calendar endCalendar = Calendar.getInstance();
		int diffYear = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
		int diffMonth = diffYear * 12 + endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);

		// sql="CREATE OR REPLACE view repo_download_stats_monthly AS select d.id, d.new_date AS month, case when
		// rdm.sum is null then 0 else rdm.sum end, d.source from (select distinct rdsm.id, to_char(date_trunc('month',
		// ('2016-01-01'::date + interval '1 month'*offs)), 'YYYY/MM') AS new_date, rdsm.source from generate_series(0,
		// " + diffMonth +", 1) AS offs, repo_download_stats_monthly_clean rdsm) d LEFT JOIN (select id, month, sum,
		// source from repo_download_stats_monthly_clean) rdm ON d.new_date=rdm.month and d.id=rdm.id and
		// d.source=rdm.source order by d.id, d.new_date";
		// sql = "select d.id, d.new_date AS month, case when rdm.sum is null then 0 else rdm.sum end, d.source INTO
		// repo_download_stats_monthly from (select distinct rdsm.id, to_char(date_trunc('month', ('2016-01-01'::date +
		// interval '1 month'*offs)), 'YYYY/MM') AS new_date, rdsm.source from generate_series(0, " + diffMonth + ", 1)
		// AS offs, repo_download_stats_monthly_clean rdsm) d LEFT JOIN (select id, month, sum, source from
		// repo_download_stats_monthly_clean) rdm ON d.new_date=rdm.month and d.id=rdm.id and d.source=rdm.source order
		// by d.id, d.new_date";
		sql = "CREATE TABLE IF NOT EXISTS repo_download_stats_monthly AS select d.id, d.new_date AS month, case when rdm.sum is null then 0 else rdm.sum end, d.source from (select distinct rdsm.id, to_char(date_trunc('month', ('2016-01-01'::date + interval '1 month'*offs)), 'YYYY/MM') AS new_date, rdsm.source from generate_series(0, "
			+ diffMonth
			+ ", 1) AS offs, repo_download_stats_monthly_clean rdsm) d LEFT JOIN (select id, month, sum, source from repo_download_stats_monthly_clean) rdm ON d.new_date=rdm.month and d.id=rdm.id and d.source=rdm.source order by d.id, d.new_date";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_monthly_id ON repo_download_stats_monthly USING btree (id)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_monthly_month ON repo_download_stats_monthly USING btree(month)";
		stmt.executeUpdate(sql);

		sql = "CREATE INDEX repo_download_stats_monthly_source ON repo_download_stats_monthly USING btree(source)";
		stmt.executeUpdate(sql);

		sql = "CREATE OR REPLACE view repo_download_stats_monthly_sushi AS SELECT id, sum(number_of_downloads), extract('year' from date) ||'-'|| LPAD(CAST(extract('month' from date) AS VARCHAR), 2, '0') ||'-01' AS month, source FROM repo_download_stats group by id, month, source;";
		stmt.executeUpdate(sql);

		stmt.close();
		ConnectDB.getHiveConnection().commit();
		ConnectDB.getHiveConnection().close();
	}

	public void processPortalLog() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Adding JSON Serde jar");
		stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
		logger.info("Added JSON Serde jar");

		logger.info("Dropping process_portal_log_tmp_json table");
		String drop_process_portal_log_tmp_json = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".process_portal_log_tmp_json";
		stmt.executeUpdate(drop_process_portal_log_tmp_json);
		logger.info("Dropped process_portal_log_tmp_json table");

		logger.info("Creating process_portal_log_tmp_json");
		String create_process_portal_log_tmp_json = "CREATE EXTERNAL TABLE IF NOT EXISTS " +
			ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp_json(" +
			"	`idSite` STRING,\n" +
			"	`idVisit` STRING,\n" +
			"	`country` STRING,\n" +
			"	`referrerName` STRING,\n" +
			"	`browser` STRING,\n" +
			"	`actionDetails` ARRAY<\n" +
			"						struct<\n" +
			"							type: STRING,\n" +
			"							url: STRING,\n" +
			"							timestamp: String\n" +
			"							>\n" +
			"						>\n" +
			")\n" +
			"ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
			"LOCATION '" + ExecuteWorkflow.portalLogPath + "'\n" +
			"TBLPROPERTIES (\"transactional\"=\"false\")";
		stmt.executeUpdate(create_process_portal_log_tmp_json);
		logger.info("Created process_portal_log_tmp_json");

		logger.info("Droping process_portal_log_tmp table");
		String drop_process_portal_log_tmp = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".process_portal_log_tmp";
		stmt.executeUpdate(drop_process_portal_log_tmp);
		logger.info("Dropped process_portal_log_tmp");

		logger.info("Creating process_portal_log_tmp");
		String create_process_portal_log_tmp = "CREATE TABLE " +
			ConnectDB.getUsageStatsDBSchema() +
			".process_portal_log_tmp (source BIGINT, id_visit STRING, country STRING, action STRING, url STRING, " +
			"entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) " +
			"clustered by (source, id_visit, timestamp) into 100 buckets stored as orc tblproperties('transactional'='true')";
		stmt.executeUpdate(create_process_portal_log_tmp);
		logger.info("Created process_portal_log_tmp");

		logger.info("Inserting into process_portal_log_tmp");
		String insert_process_portal_log_tmp = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema()
			+ ".process_portal_log_tmp " +
			"SELECT DISTINCT cast(idSite as BIGINT) as source, idVisit  as id_Visit, country, actiondetail.type as action, "
			+
			"actiondetail.url as url, " +
			"CASE\n" +
			"  WHEN (actiondetail.url like '%datasourceId=%') THEN split(actiondetail.url,'datasourceId=')[1] " +
			"  WHEN (actiondetail.url like '%datasource=%') THEN split(actiondetail.url,'datasource=')[1] " +
			"  WHEN (actiondetail.url like '%datasourceFilter=%') THEN split(actiondetail.url,'datasourceFilter=')[1] "
			+
			"  WHEN (actiondetail.url like '%articleId=%') THEN split(actiondetail.url,'articleId=')[1] " +
			"  WHEN (actiondetail.url like '%datasetId=%') THEN split(actiondetail.url,'datasetId=')[1] " +
			"  WHEN (actiondetail.url like '%projectId=%') THEN split(actiondetail.url,'projectId=')[1] " +
			"  WHEN (actiondetail.url like '%organizationId=%') THEN split(actiondetail.url,'organizationId=')[1] " +
			"  ELSE '' " +
			"END AS entity_id, " +
			"CASE " +
			"  WHEN (actiondetail.url like '%datasourceId=%') THEN 'datasource' " +
			"  WHEN (actiondetail.url like '%datasource=%') THEN 'datasource' " +
			"  WHEN (actiondetail.url like '%datasourceFilter=%') THEN 'datasource' " +
			"  WHEN (actiondetail.url like '%articleId=%') THEN 'result' " +
			"  WHEN (actiondetail.url like '%datasetId=%') THEN 'result' " +
			"  WHEN (actiondetail.url like '%projectId=%') THEN 'project' " +
			"  WHEN (actiondetail.url like '%organizationId=%') THEN 'organization' " +
			"  ELSE '' " +
			"END AS source_item_type, " +
			"from_unixtime(cast(actiondetail.timestamp as BIGINT)) as timestamp, referrerName as referrer_name, " +
			"browser as agent " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp_json " +
			"LATERAL VIEW explode(actiondetails) actiondetailsTable AS actiondetail";
		stmt.executeUpdate(insert_process_portal_log_tmp);
		logger.info("Inserted into process_portal_log_tmp");

		stmt.close();
	}

	public void portalStats() throws SQLException {
		Connection con = ConnectDB.getHiveConnection();
		Statement stmt = con.createStatement();
		con.setAutoCommit(false);

//		Original queries where of the style
//		
//		SELECT DISTINCT source, id_visit, country, action, url, roid.oid, 'oaItem', `timestamp`, referrer_name, agent 
//		FROM usagestats_20200907.process_portal_log_tmp2, 
//		openaire_prod_stats_20200821.result_oids roid 
//		WHERE entity_id IS NOT null AND entity_id=roid.oid AND roid.oid IS NOT null
//		
//		The following query is an example of how queries should be 
//		
//		
//		INSERT INTO usagestats_20200907.piwiklogtmp
//		SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'oaItem', `timestamp`, referrer_name, agent
//		FROM usagestats_20200907.process_portal_log_tmp
//		WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id
//		IN (SELECT roid.oid FROM openaire_prod_stats_20200821.result_oids roid WHERE roid.oid IS NOT NULL);		
//		
//		We should consider if we would like the queries to be as the following
//		
//		INSERT INTO usagestats_20200907.piwiklogtmp
//		SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'oaItem', `timestamp`, referrer_name, agent
//		FROM usagestats_20200907.process_portal_log_tmp
//		WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id != '' AND process_portal_log_tmp.entity_id
//		IN (SELECT roid.oid FROM openaire_prod_stats_20200821.result_oids roid WHERE roid.oid IS NOT NULL AND
//		roid.oid != '');		

		logger.info("PortalStats - Step 1");
		String sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'oaItem', `timestamp`, referrer_name, agent "
			+
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp " +
			"WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id " +
			"IN (SELECT roid.id FROM " + ConnectDB.getStatsDBSchema()
			+ ".result_oids roid WHERE roid.id IS NOT NULL)";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("PortalStats - Step 2");
		stmt = con.createStatement();
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'datasource', `timestamp`, referrer_name, agent "
			+
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp " +
			"WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id " +
			"IN (SELECT roid.id FROM " + ConnectDB.getStatsDBSchema()
			+ ".datasource_oids roid WHERE roid.id IS NOT NULL)";
		stmt.executeUpdate(sql);
		stmt.close();

		/*
		 * logger.info("PortalStats - Step 3"); stmt = con.createStatement(); sql = "INSERT INTO " +
		 * ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
		 * "SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'organization', `timestamp`, referrer_name, agent "
		 * + "FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp " +
		 * "WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id " +
		 * "IN (SELECT roid.id FROM " + ConnectDB.getStatsDBSchema() +
		 * ".organization_oids roid WHERE roid.id IS NOT NULL)"; // stmt.executeUpdate(sql); stmt.close();
		 */
		logger.info("PortalStats - Step 3");
		stmt = con.createStatement();
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'project', `timestamp`, referrer_name, agent "
			+
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp " +
			"WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id " +
			"IN (SELECT roid.id FROM " + ConnectDB.getStatsDBSchema()
			+ ".project_oids roid WHERE roid.id IS NOT NULL)";
		stmt.executeUpdate(sql);
		stmt.close();

		con.close();
	}

	private void cleanOAI() throws Exception {
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Cleaning oai - Step 1");
		stmt = ConnectDB.getHiveConnection().createStatement();
		String sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.chlc.min-saude.pt/'," +
			"'oai:repositorio.chlc.min-saude.pt:') WHERE entity_id LIKE 'oai:repositorio.chlc.min-saude.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 2");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.hospitaldebraga.pt/'," +
			"'oai:repositorio.hospitaldebraga.pt:') WHERE entity_id LIKE 'oai:repositorio.hospitaldebraga.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 3");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipl.pt/'," +
			"'oai:repositorio.ipl.pt:') WHERE entity_id LIKE 'oai:repositorio.ipl.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 4");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:bibliotecadigital.ipb.pt/'," +
			"'oai:bibliotecadigital.ipb.pt:') WHERE entity_id LIKE 'oai:bibliotecadigital.ipb.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 5");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ismai.pt/'," +
			"'oai:repositorio.ismai.pt:') WHERE entity_id LIKE 'oai:repositorio.ismai.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 6");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorioaberto.uab.pt/'," +
			"'oai:repositorioaberto.uab.pt:') WHERE entity_id LIKE 'oai:repositorioaberto.uab.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 7");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.uac.pt/'," +
			"'oai:repositorio.uac.pt:') WHERE entity_id LIKE 'oai:repositorio.uac.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 8");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.insa.pt/'," +
			"'oai:repositorio.insa.pt:') WHERE entity_id LIKE 'oai:repositorio.insa.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 9");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipcb.pt/'," +
			"'oai:repositorio.ipcb.pt:') WHERE entity_id LIKE 'oai:repositorio.ipcb.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 10");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ispa.pt/'," +
			"'oai:repositorio.ispa.pt:') WHERE entity_id LIKE 'oai:repositorio.ispa.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 11");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.chporto.pt/'," +
			"'oai:repositorio.chporto.pt:') WHERE entity_id LIKE 'oai:repositorio.chporto.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 12");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ucp.pt/'," +
			"'oai:repositorio.ucp.pt:') WHERE entity_id LIKE 'oai:repositorio.ucp.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 13");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:rihuc.huc.min-saude.pt/'," +
			"'oai:rihuc.huc.min-saude.pt:') WHERE entity_id LIKE 'oai:rihuc.huc.min-saude.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 14");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipv.pt/'," +
			"'oai:repositorio.ipv.pt:') WHERE entity_id LIKE 'oai:repositorio.ipv.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 15");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:www.repository.utl.pt/'," +
			"'oai:www.repository.utl.pt:') WHERE entity_id LIKE 'oai:www.repository.utl.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 16");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:run.unl.pt/'," +
			"'oai:run.unl.pt:') WHERE entity_id LIKE 'oai:run.unl.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 17");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:sapientia.ualg.pt/'," +
			"'oai:sapientia.ualg.pt:') WHERE entity_id LIKE 'oai:sapientia.ualg.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 18");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipsantarem.pt/'," +
			"'oai:repositorio.ipsantarem.pt:') WHERE entity_id LIKE 'oai:repositorio.ipsantarem.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 19");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:arca.igc.gulbenkian.pt/'," +
			"'oai:arca.igc.gulbenkian.pt:') WHERE entity_id LIKE 'oai:arca.igc.gulbenkian.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 20");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:ubibliorum.ubi.pt/'," +
			"'oai:ubibliorum.ubi.pt:') WHERE entity_id LIKE 'oai:ubibliorum.ubi.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 21");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:digituma.uma.pt/'," +
			"'oai:digituma.uma.pt:') WHERE entity_id LIKE 'oai:digituma.uma.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 22");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ul.pt/'," +
			"'oai:repositorio.ul.pt:') WHERE entity_id LIKE 'oai:repositorio.ul.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 23");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.hff.min-saude.pt/'," +
			"'oai:repositorio.hff.min-saude.pt:') WHERE entity_id LIKE 'oai:repositorio.hff.min-saude.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 24");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorium.sdum.uminho.pt/'," +
			"'oai:repositorium.sdum.uminho.pt:') WHERE entity_id LIKE 'oai:repositorium.sdum.uminho.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 25");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:recipp.ipp.pt/'," +
			"'oai:recipp.ipp.pt:') WHERE entity_id LIKE 'oai:recipp.ipp.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 26");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:bdigital.ufp.pt/'," +
			"'oai:bdigital.ufp.pt:') WHERE entity_id LIKE 'oai:bdigital.ufp.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 27");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:repositorio.lneg.pt/'," +
			"'oai:repositorio.lneg.pt:') WHERE entity_id LIKE 'oai:repositorio.lneg.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 28");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:iconline.ipleiria.pt/'," +
			"'oai:iconline.ipleiria.pt:') WHERE entity_id LIKE 'oai:iconline.ipleiria.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 29");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp " +
			"SET entity_id = regexp_replace(entity_id, '^oai:comum.rcaap.pt/'," +
			"'oai:comum.rcaap.pt:') WHERE entity_id LIKE 'oai:comum.rcaap.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Done, closing connection");
		ConnectDB.getHiveConnection().close();
	}

	private String processPortalURL(String url) {

		if (url.indexOf("explore.openaire.eu") > 0) {
			try {
				url = URLDecoder.decode(url, "UTF-8");
			} catch (Exception e) {
				logger.info("Error when decoding the following URL: " + url);
			}
			if (url.indexOf("datasourceId=") > 0 && url.substring(url.indexOf("datasourceId=") + 13).length() >= 46) {
				url = "datasource|"
					+ url.substring(url.indexOf("datasourceId=") + 13, url.indexOf("datasourceId=") + 59);
			} else if (url.indexOf("datasource=") > 0
				&& url.substring(url.indexOf("datasource=") + 11).length() >= 46) {
				url = "datasource|" + url.substring(url.indexOf("datasource=") + 11, url.indexOf("datasource=") + 57);
			} else if (url.indexOf("datasourceFilter=") > 0
				&& url.substring(url.indexOf("datasourceFilter=") + 17).length() >= 46) {
				url = "datasource|"
					+ url.substring(url.indexOf("datasourceFilter=") + 17, url.indexOf("datasourceFilter=") + 63);
			} else if (url.indexOf("articleId=") > 0 && url.substring(url.indexOf("articleId=") + 10).length() >= 46) {
				url = "result|" + url.substring(url.indexOf("articleId=") + 10, url.indexOf("articleId=") + 56);
			} else if (url.indexOf("datasetId=") > 0 && url.substring(url.indexOf("datasetId=") + 10).length() >= 46) {
				url = "result|" + url.substring(url.indexOf("datasetId=") + 10, url.indexOf("datasetId=") + 56);
			} else if (url.indexOf("projectId=") > 0 && url.substring(url.indexOf("projectId=") + 10).length() >= 46
				&& !url.contains("oai:dnet:corda")) {
				url = "project|" + url.substring(url.indexOf("projectId=") + 10, url.indexOf("projectId=") + 56);
			} else if (url.indexOf("organizationId=") > 0
				&& url.substring(url.indexOf("organizationId=") + 15).length() >= 46) {
				url = "organization|"
					+ url.substring(url.indexOf("organizationId=") + 15, url.indexOf("organizationId=") + 61);
			} else {
				url = "";
			}
		} else {
			url = "";
		}

		return url;
	}

	private void updateProdTables() throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Inserting data to piwiklog");
		String sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklog " +
			"SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp";
		stmt.executeUpdate(sql);

		logger.info("Inserting data to views_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".views_stats " +
			"SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".views_stats_tmp";
		stmt.executeUpdate(sql);

		logger.info("Inserting data to downloads_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats " +
			"SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats_tmp";
		stmt.executeUpdate(sql);

		logger.info("Inserting data to pageviews_stats");
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats " +
			"SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats_tmp";
		stmt.executeUpdate(sql);

		/*
		 * logger.info("Dropping table views_stats_tmp"); sql = "DROP TABLE IF EXISTS " +
		 * ConnectDB.getUsageStatsDBSchema() + ".views_stats_tmp"; stmt.executeUpdate(sql);
		 * logger.info("Dropping table downloads_stats_tmp"); sql = "DROP TABLE IF EXISTS " +
		 * ConnectDB.getUsageStatsDBSchema() + ".downloads_stats_tmp"; stmt.executeUpdate(sql);
		 * logger.info("Dropping table pageviews_stats_tmp"); sql = "DROP TABLE IF EXISTS " +
		 * ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats_tmp"; stmt.executeUpdate(sql);
		 * logger.info("Dropping table process_portal_log_tmp"); sql = "DROP TABLE IF EXISTS " +
		 * ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp"; stmt.executeUpdate(sql);
		 */
		stmt.close();
		ConnectDB.getHiveConnection().close();

	}

	private ArrayList<String> listHdfsDir(String dir) throws Exception {

		FileSystem hdfs = FileSystem.get(new Configuration());
		RemoteIterator<LocatedFileStatus> Files;
		ArrayList<String> fileNames = new ArrayList<>();

		try {
			Path exportPath = new Path(hdfs.getUri() + dir);
			Files = hdfs.listFiles(exportPath, false);
			while (Files.hasNext()) {
				String fileName = Files.next().getPath().toString();
				fileNames.add(fileName);
			}

			hdfs.close();
		} catch (Exception e) {
			logger.error("HDFS file path with exported data does not exist : " + new Path(hdfs.getUri() + logPath));
			throw new Exception("HDFS file path with exported data does not exist :   " + logPath, e);
		}

		return fileNames;
	}

	private String readHDFSFile(String filename) throws Exception {
		String result;
		try {

			FileSystem fs = FileSystem.get(new Configuration());
			// log.info("reading file : " + filename);

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filename))));

			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				if (!line.equals("[]")) {
					sb.append(line);
				}
				// sb.append(line);
				line = br.readLine();
			}
			result = sb.toString().replace("][{\"idSite\"", ",{\"idSite\"");
			if (result.equals("")) {
				result = "[]";
			}

			// fs.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e);
		}

		return result;
	}

	private Connection getConnection() throws SQLException {
		return ConnectDB.getHiveConnection();
	}
}
