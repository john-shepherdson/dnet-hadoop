
package eu.dnetlib.oa.graph.usagerawdata.export;

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
		//////////// this.createTmpTables();
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

//            String dropT = "TRUNCATE TABLE "
//                    + ConnectDB.getUsageStatsDBSchema()
//                    + ".piwiklog ";
//            stmt.executeUpdate(dropT);
//                        logger.info("truncated piwiklog");

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
			portalLogs();
			logger.info("Portal usagestats process done");

			logger.info("Updating Production Tables");
			updateProdTables();
			logger.info("Updated Production Tables");

			logger.info("Create Pedocs Tables");
			createPedocsOldUsageData();
			logger.info("Pedocs Tables Created");

			logger.info("Create Datacite Tables");
			createDatasetsUsageData();
			logger.info("Datacite Tables Created");

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
		String drop_piwiklogtmp_json = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklogtmp_json";
		stmt.executeUpdate(drop_piwiklogtmp_json);
		logger.info("Dropped piwiklogtmp_json table");

		logger.info("Creating piwiklogtmp_json");
		String create_piwiklogtmp_json = "CREATE EXTERNAL TABLE IF NOT EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklogtmp_json(\n"
			+ "	`idSite` STRING,\n"
			+ "	`idVisit` STRING,\n"
			+ "	`country` STRING,\n"
			+ "	`referrerName` STRING,\n"
			+ "	`browser` STRING,\n"
			+ "	`actionDetails` ARRAY<\n"
			+ "						struct<\n"
			+ "							type: STRING,\n"
			+ "							url: STRING,\n"
			+ "							`customVariables`: struct<\n"
			+ "								`1`: struct<\n"
			+ "								`customVariablePageValue1`: STRING\n"
			+ "										>\n"
			+ "								>,\n"
			+ "							timestamp: String\n"
			+ "							>\n"
			+ "						>\n"
			+ ")\n"
			+ "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n"
			+ "LOCATION '" + ExecuteWorkflow.repoLogPath + "'\n"
			+ "TBLPROPERTIES (\"transactional\"=\"false\")";
		stmt.executeUpdate(create_piwiklogtmp_json);
		logger.info("Created piwiklogtmp_json");

		logger.info("Dropping piwiklogtmp table");
		String drop_piwiklogtmp = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklogtmp";
		stmt.executeUpdate(drop_piwiklogtmp);
		logger.info("Dropped piwiklogtmp");

		logger.info("Creating piwiklogtmp");
		String create_piwiklogtmp = "CREATE TABLE "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklogtmp (source BIGINT, id_Visit STRING, country STRING, action STRING, url STRING, "
			+ "entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING)  "
			+ "clustered by (source) into 100 buckets stored as orc tblproperties('transactional'='true')";
		stmt.executeUpdate(create_piwiklogtmp);
		logger.info("Created piwiklogtmp");

		logger.info("Inserting into piwiklogtmp");
		String insert_piwiklogtmp = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SELECT DISTINCT cast(idSite as BIGINT) as source, idVisit  as id_Visit, country, "
			+ "actiondetail.type as action, actiondetail.url as url, "
			+ "actiondetail.customVariables.`1`.`customVariablePageValue1` as entity_id, "
			+ "'repItem' as source_item_type, from_unixtime(cast(actiondetail.timestamp as BIGINT)) as timestamp, "
			+ "referrerName as referrer_name, browser as agent\n"
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp_json\n"
			+ "LATERAL VIEW explode(actiondetails) actiondetailsTable AS actiondetail";
		stmt.executeUpdate(insert_piwiklogtmp);
		logger.info("Inserted into piwiklogtmp");

		stmt.close();
	}

	public void removeDoubleClicks() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Cleaning download double clicks");
		// clean download double clicks
		String sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "WHERE EXISTS (\n"
			+ "SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp \n"
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p1, "
			+ ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p2\n"
			+ "WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id \n"
			+ "AND p1.action=p2.action AND p1.action='download' AND p1.timestamp!=p2.timestamp \n"
			+ "AND p1.timestamp<p2.timestamp AND ((unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))/60)<30 \n"
			+ "AND piwiklogtmp.source=p1.source AND piwiklogtmp.id_visit=p1.id_visit \n"
			+ "AND piwiklogtmp.action=p1.action AND piwiklogtmp.entity_id=p1.entity_id AND piwiklogtmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		logger.info("Cleaned download double clicks");

		// clean view double clicks
		logger.info("Cleaning action double clicks");
		ConnectDB.getHiveConnection().setAutoCommit(false);
		sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "WHERE EXISTS (\n"
			+ "SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp \n"
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p1, "
			+ ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp p2\n"
			+ "WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id \n"
			+ "AND p1.action=p2.action AND p1.action='action' AND p1.timestamp!=p2.timestamp \n"
			+ "AND p1.timestamp<p2.timestamp AND (unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))<10 \n"
			+ "AND piwiklogtmp.source=p1.source AND piwiklogtmp.id_visit=p1.id_visit \n"
			+ "AND piwiklogtmp.action=p1.action AND piwiklogtmp.entity_id=p1.entity_id AND piwiklogtmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		logger.info("Cleaned action double clicks");
		stmt.close();
	}

	public void processPortalLog() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Adding JSON Serde jar");
		stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
		logger.info("Added JSON Serde jar");

		logger.info("Dropping process_portal_log_tmp_json table");
		String drop_process_portal_log_tmp_json = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".process_portal_log_tmp_json";
		stmt.executeUpdate(drop_process_portal_log_tmp_json);
		logger.info("Dropped process_portal_log_tmp_json table");

		logger.info("Creating process_portal_log_tmp_json");
		String create_process_portal_log_tmp_json = "CREATE EXTERNAL TABLE IF NOT EXISTS "
			+ ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp_json("
			+ "	`idSite` STRING,\n"
			+ "	`idVisit` STRING,\n"
			+ "	`country` STRING,\n"
			+ "	`referrerName` STRING,\n"
			+ "	`browser` STRING,\n"
			+ "	`actionDetails` ARRAY<\n"
			+ "						struct<\n"
			+ "							type: STRING,\n"
			+ "							url: STRING,\n"
			+ "							timestamp: String\n"
			+ "							>\n"
			+ "						>\n"
			+ ")\n"
			+ "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n"
			+ "LOCATION '" + ExecuteWorkflow.portalLogPath + "'\n"
			+ "TBLPROPERTIES (\"transactional\"=\"false\")";
		stmt.executeUpdate(create_process_portal_log_tmp_json);
		logger.info("Created process_portal_log_tmp_json");

		logger.info("Droping process_portal_log_tmp table");
		String drop_process_portal_log_tmp = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".process_portal_log_tmp";
		stmt.executeUpdate(drop_process_portal_log_tmp);
		logger.info("Dropped process_portal_log_tmp");

		logger.info("Creating process_portal_log_tmp");
		String create_process_portal_log_tmp = "CREATE TABLE "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".process_portal_log_tmp (source BIGINT, id_visit STRING, country STRING, action STRING, url STRING, "
			+ "entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) "
			+ "clustered by (source, id_visit, timestamp) into 100 buckets stored as orc tblproperties('transactional'='true')";
		stmt.executeUpdate(create_process_portal_log_tmp);
		logger.info("Created process_portal_log_tmp");

		logger.info("Inserting into process_portal_log_tmp");
		String insert_process_portal_log_tmp = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema()
			+ ".process_portal_log_tmp "
			+ "SELECT DISTINCT cast(idSite as BIGINT) as source, idVisit  as id_Visit, country, actiondetail.type as action, "
			+ "actiondetail.url as url, "
			+ "CASE\n"
			+ "  WHEN (actiondetail.url like '%datasourceId=%') THEN split(actiondetail.url,'datasourceId=')[1] "
			+ "  WHEN (actiondetail.url like '%datasource=%') THEN split(actiondetail.url,'datasource=')[1] "
			+ "  WHEN (actiondetail.url like '%datasourceFilter=%') THEN split(actiondetail.url,'datasourceFilter=')[1] "
			+ "  WHEN (actiondetail.url like '%articleId=%') THEN split(actiondetail.url,'articleId=')[1] "
			+ "  WHEN (actiondetail.url like '%datasetId=%') THEN split(actiondetail.url,'datasetId=')[1] "
			+ "  WHEN (actiondetail.url like '%projectId=%') THEN split(actiondetail.url,'projectId=')[1] "
			+ "  WHEN (actiondetail.url like '%organizationId=%') THEN split(actiondetail.url,'organizationId=')[1] "
			+ "  ELSE '' "
			+ "END AS entity_id, "
			+ "CASE "
			+ "  WHEN (actiondetail.url like '%datasourceId=%') THEN 'datasource' "
			+ "  WHEN (actiondetail.url like '%datasource=%') THEN 'datasource' "
			+ "  WHEN (actiondetail.url like '%datasourceFilter=%') THEN 'datasource' "
			+ "  WHEN (actiondetail.url like '%articleId=%') THEN 'result' "
			+ "  WHEN (actiondetail.url like '%datasetId=%') THEN 'result' "
			+ "  WHEN (actiondetail.url like '%projectId=%') THEN 'project' "
			+ "  WHEN (actiondetail.url like '%organizationId=%') THEN 'organization' "
			+ "  ELSE '' "
			+ "END AS source_item_type, "
			+ "from_unixtime(cast(actiondetail.timestamp as BIGINT)) as timestamp, referrerName as referrer_name, "
			+ "browser as agent "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp_json "
			+ "LATERAL VIEW explode(actiondetails) actiondetailsTable AS actiondetail";
		stmt.executeUpdate(insert_process_portal_log_tmp);
		logger.info("Inserted into process_portal_log_tmp");

		stmt.close();
	}

	public void portalLogs() throws SQLException {
		Connection con = ConnectDB.getHiveConnection();
		Statement stmt = con.createStatement();
		con.setAutoCommit(false);

		logger.info("PortalStats - Step 1");
		String sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'oaItem', `timestamp`, referrer_name, agent "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp "
			+ "WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id "
			+ "IN (SELECT roid.id FROM " + ConnectDB.getStatsDBSchema()
			+ ".result_oids roid WHERE roid.id IS NOT NULL)";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("PortalStats - Step 2");
		stmt = con.createStatement();
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'datasource', `timestamp`, referrer_name, agent "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp "
			+ "WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id "
			+ "IN (SELECT roid.id FROM " + ConnectDB.getStatsDBSchema()
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
		sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SELECT DISTINCT source, id_visit, country, action, url, entity_id, 'project', `timestamp`, referrer_name, agent "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp "
			+ "WHERE process_portal_log_tmp.entity_id IS NOT NULL AND process_portal_log_tmp.entity_id "
			+ "IN (SELECT roid.id FROM " + ConnectDB.getStatsDBSchema()
			+ ".project_oids roid WHERE roid.id IS NOT NULL)";
		stmt.executeUpdate(sql);
		stmt.close();

		con.close();
	}

	private void cleanOAI() throws Exception {
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Cleaning oai - Step 1");
		stmt = ConnectDB.getHiveConnection().createStatement();
		String sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.chlc.min-saude.pt/',"
			+ "'oai:repositorio.chlc.min-saude.pt:') WHERE entity_id LIKE 'oai:repositorio.chlc.min-saude.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 2");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.hospitaldebraga.pt/',"
			+ "'oai:repositorio.hospitaldebraga.pt:') WHERE entity_id LIKE 'oai:repositorio.hospitaldebraga.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 3");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipl.pt/',"
			+ "'oai:repositorio.ipl.pt:') WHERE entity_id LIKE 'oai:repositorio.ipl.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 4");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:bibliotecadigital.ipb.pt/',"
			+ "'oai:bibliotecadigital.ipb.pt:') WHERE entity_id LIKE 'oai:bibliotecadigital.ipb.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 5");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ismai.pt/',"
			+ "'oai:repositorio.ismai.pt:') WHERE entity_id LIKE 'oai:repositorio.ismai.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 6");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorioaberto.uab.pt/',"
			+ "'oai:repositorioaberto.uab.pt:') WHERE entity_id LIKE 'oai:repositorioaberto.uab.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 7");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.uac.pt/',"
			+ "'oai:repositorio.uac.pt:') WHERE entity_id LIKE 'oai:repositorio.uac.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 8");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.insa.pt/',"
			+ "'oai:repositorio.insa.pt:') WHERE entity_id LIKE 'oai:repositorio.insa.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 9");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipcb.pt/',"
			+ "'oai:repositorio.ipcb.pt:') WHERE entity_id LIKE 'oai:repositorio.ipcb.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 10");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ispa.pt/',"
			+ "'oai:repositorio.ispa.pt:') WHERE entity_id LIKE 'oai:repositorio.ispa.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 11");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.chporto.pt/',"
			+ "'oai:repositorio.chporto.pt:') WHERE entity_id LIKE 'oai:repositorio.chporto.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 12");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ucp.pt/',"
			+ "'oai:repositorio.ucp.pt:') WHERE entity_id LIKE 'oai:repositorio.ucp.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 13");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:rihuc.huc.min-saude.pt/',"
			+ "'oai:rihuc.huc.min-saude.pt:') WHERE entity_id LIKE 'oai:rihuc.huc.min-saude.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 14");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipv.pt/',"
			+ "'oai:repositorio.ipv.pt:') WHERE entity_id LIKE 'oai:repositorio.ipv.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 15");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:www.repository.utl.pt/',"
			+ "'oai:www.repository.utl.pt:') WHERE entity_id LIKE 'oai:www.repository.utl.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 16");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:run.unl.pt/',"
			+ "'oai:run.unl.pt:') WHERE entity_id LIKE 'oai:run.unl.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 17");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:sapientia.ualg.pt/',"
			+ "'oai:sapientia.ualg.pt:') WHERE entity_id LIKE 'oai:sapientia.ualg.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 18");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ipsantarem.pt/',"
			+ "'oai:repositorio.ipsantarem.pt:') WHERE entity_id LIKE 'oai:repositorio.ipsantarem.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 19");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:arca.igc.gulbenkian.pt/',"
			+ "'oai:arca.igc.gulbenkian.pt:') WHERE entity_id LIKE 'oai:arca.igc.gulbenkian.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 20");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:ubibliorum.ubi.pt/',"
			+ "'oai:ubibliorum.ubi.pt:') WHERE entity_id LIKE 'oai:ubibliorum.ubi.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 21");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:digituma.uma.pt/',"
			+ "'oai:digituma.uma.pt:') WHERE entity_id LIKE 'oai:digituma.uma.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 22");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.ul.pt/',"
			+ "'oai:repositorio.ul.pt:') WHERE entity_id LIKE 'oai:repositorio.ul.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 23");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.hff.min-saude.pt/',"
			+ "'oai:repositorio.hff.min-saude.pt:') WHERE entity_id LIKE 'oai:repositorio.hff.min-saude.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 24");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorium.sdum.uminho.pt/',"
			+ "'oai:repositorium.sdum.uminho.pt:') WHERE entity_id LIKE 'oai:repositorium.sdum.uminho.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 25");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:recipp.ipp.pt/',"
			+ "'oai:recipp.ipp.pt:') WHERE entity_id LIKE 'oai:recipp.ipp.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 26");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:bdigital.ufp.pt/',"
			+ "'oai:bdigital.ufp.pt:') WHERE entity_id LIKE 'oai:bdigital.ufp.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 27");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:repositorio.lneg.pt/',"
			+ "'oai:repositorio.lneg.pt:') WHERE entity_id LIKE 'oai:repositorio.lneg.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 28");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:iconline.ipleiria.pt/',"
			+ "'oai:iconline.ipleiria.pt:') WHERE entity_id LIKE 'oai:iconline.ipleiria.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Step 29");
		stmt = ConnectDB.getHiveConnection().createStatement();
		sql = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp "
			+ "SET entity_id = regexp_replace(entity_id, '^oai:comum.rcaap.pt/',"
			+ "'oai:comum.rcaap.pt:') WHERE entity_id LIKE 'oai:comum.rcaap.pt/%'";
		stmt.executeUpdate(sql);
		stmt.close();

		logger.info("Cleaning oai - Done, closing connection");
		ConnectDB.getHiveConnection().close();
	}

	private void updateProdTables() throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Inserting data to piwiklog");
		String sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklog "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp";
		stmt.executeUpdate(sql);

		logger.info("Dropping piwiklogtmp");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped piwiklogtmp");

		logger.info("Dropping process_portal_log_tmp");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped process_portal_log_tmp");

		stmt.close();
		ConnectDB.getHiveConnection().close();

	}

	public void finalizeStats() throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Dropping piwiklogtmp");
		String sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped piwiklogtmp");

		logger.info("Dropping process_portal_log_tmp");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped process_portal_log_tmp");

		logger.info("Dropping irus_sushilogtmp");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".irus_sushilogtmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped irus_sushilogtmp");

		logger.info("Dropping irus_sushilogtmp_json");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".irus_sushilogtmp_json";
		stmt.executeUpdate(sql);
		logger.info("Dropped irus_sushilogtmp_json");

		logger.info("Dropping lareferencialogtmp_json");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp_json";
		stmt.executeUpdate(sql);
		logger.info("Dropped lareferencialogtmp_json");

		logger.info("Dropping piwiklogtmp_json");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogtmp_json";
		stmt.executeUpdate(sql);
		logger.info("Dropped piwiklogtmp_json");

		logger.info("Dropping process_portal_log_tmp_json");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".process_portal_log_tmp_json";
		stmt.executeUpdate(sql);
		logger.info("Dropped process_portal_log_tmp_json");

		logger.info("Dropping sarc_sushilogtmp");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped sarc_sushilogtmp");

		logger.info("Dropping sarc_sushilogtmp_json_array");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_json_array";
		stmt.executeUpdate(sql);
		logger.info("Dropped sarc_sushilogtmp_json_array");

		logger.info("Dropping sarc_sushilogtmp_json_non_array");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_json_non_array";
		stmt.executeUpdate(sql);
		logger.info("Dropped sarc_sushilogtmp_json_non_array");

		logger.info("Dropping piwiklogb2sharetmp");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp";
		stmt.executeUpdate(sql);
		logger.info("Dropped piwiklogb2sharetmp");

		logger.info("Dropping piwiklog_b2share_tmp_json");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklog_b2share_tmp_json";
		stmt.executeUpdate(sql);
		logger.info("Dropped piwiklog_b2share_tmp_json");

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

	public void createPedocsOldUsageData() throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Creating PeDocs Old Views Table");
		String sql = "Create TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".pedocsoldviews as select * from default.pedocsviews";
		stmt.executeUpdate(sql);
		logger.info("PeDocs Old Views Table created");

		logger.info("Creating PeDocs Old Downloads Table");
		sql = "Create TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".pedocsolddownloads as select * from default.pedocsdownloads";
		stmt.executeUpdate(sql);
		logger.info("PeDocs Old Downloads Table created");

	}

	public void createDatasetsUsageData() throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Dropping datacite_views");
		String sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".datacite_views";
		stmt.executeUpdate(sql);
		logger.info("Dropped datacite_views");

		logger.info("Dropping datacite_downloads");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".datacite_downloads";
		stmt.executeUpdate(sql);
		logger.info("Dropped datacite_downloads");

		logger.info("Creating Datasets Views Table");
		sql = "Create TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".datacite_views as select * from openaire_prod_datacite_usage_stats.datacite_views";
		stmt.executeUpdate(sql);
		logger.info("Datasets Views Table created");

		logger.info("Creating Datasets Downloads Table");
		sql = "Create TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".datacite_downloads as select * from openaire_prod_datacite_usage_stats.datacite_downloads";
		stmt.executeUpdate(sql);
		logger.info("Datasets Downloads Table created");

	}
}
