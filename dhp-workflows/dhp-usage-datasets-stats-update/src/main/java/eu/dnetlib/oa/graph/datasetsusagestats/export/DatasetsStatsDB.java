
package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class DatasetsStatsDB {

	private String logPath;
	private String logRepoPath;
	private String logPortalPath;

	private Statement stmt = null;

	private static final Logger logger = LoggerFactory.getLogger(DatasetsStatsDB.class);

	private String CounterRobotsURL;
	private ArrayList robotsList;

	public DatasetsStatsDB(String logRepoPath, String logPortalPath) throws Exception {
		this.logRepoPath = logRepoPath;
		this.logPortalPath = logPortalPath;

	}

	public void recreateDBAndTables() throws Exception {
		this.createDatabase();
		this.createTables();
	}

//    public void reCreateLogDirs() throws IllegalArgumentException, IOException {
//        FileSystem dfs = FileSystem.get(new Configuration());
//
//        logger.info("Deleting repoLog directory: " + ExecuteWorkflow.repoLogPath);
//        dfs.delete(new Path(ExecuteWorkflow.repoLogPath), true);
//
//        logger.info("Deleting portalLog directory: " + ExecuteWorkflow.portalLogPath);
//        dfs.delete(new Path(ExecuteWorkflow.portalLogPath), true);
//
//        logger.info("Creating repoLog directory: " + ExecuteWorkflow.repoLogPath);
//        dfs.mkdirs(new Path(ExecuteWorkflow.repoLogPath));
//
//        logger.info("Creating portalLog directory: " + ExecuteWorkflow.portalLogPath);
//        dfs.mkdirs(new Path(ExecuteWorkflow.portalLogPath));
//    }
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

			logger.info("Dropping datasets DB: " + ConnectDB.getDataSetUsageStatsDBSchema());
			String dropDatabase = "DROP DATABASE IF EXISTS " + ConnectDB.getDataSetUsageStatsDBSchema() + " CASCADE";
			stmt.executeUpdate(dropDatabase);
		} catch (Exception e) {
			logger.error("Failed to drop database: " + e);
			throw new Exception("Failed to drop database: " + e.toString(), e);
		}

		try {
			stmt = ConnectDB.getHiveConnection().createStatement();

			logger.info("Creating usagestats DB: " + ConnectDB.getDataSetUsageStatsDBSchema());
			String createDatabase = "CREATE DATABASE IF NOT EXISTS " + ConnectDB.getDataSetUsageStatsDBSchema();
			stmt.executeUpdate(createDatabase);

		} catch (Exception e) {
			logger.error("Failed to create database: " + e);
			throw new Exception("Failed to create database: " + e.toString(), e);
		}
	}

	private void createTables() throws Exception {
		try {
			stmt = ConnectDB.getHiveConnection().createStatement();

			// Create Reports table - This table should exist
			logger.info("Creating Reports Table");
			String sqlCreateTableDataciteReports = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getDataSetUsageStatsDBSchema()
				+ ".datacitereports(reportid STRING, \n"
				+ "	name STRING, \n"
				+ "    source STRING,\n"
				+ "    release STRING,\n"
				+ "    createdby STRING,\n"
				+ "    report_start_date STRING,\n"
				+ "    report_end_date STRING)\n"
				+ "    CLUSTERED BY (reportid)\n"
				+ "	into 100 buckets stored as orc tblproperties('transactional'='true')";

			stmt.executeUpdate(sqlCreateTableDataciteReports);
			logger.info("Reports Table Created");

			// Create Datasets Table
			logger.info("Creating DataSets Table");
			String sqlCreateTableDataSets = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getDataSetUsageStatsDBSchema()
				+ ".datasets(ds_type STRING,\n"
				+ "    ds_title STRING,\n"
				+ "    yop STRING,\n"
				+ "    uri STRING,\n"
				+ "    platform STRING,\n"
				+ "    data_type STRING,\n"
				+ "    publisher STRING,\n"
				+ "    publisher_id_type STRING,\n"
				+ "    publisher_id_value STRING,\n"
				+ "    ds_dates_type STRING,\n"
				+ "    ds_pub_date STRING,\n"
				+ "    ds_contributors STRING,\n"
				// + " ds_contributor_value array <STRING>,\n"
				+ "    reportid STRING)\n"
				+ "    CLUSTERED BY (ds_type)\n"
				+ "	into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTableDataSets);
			logger.info("DataSets Table Created");

			// Create Datasets Performance Table
			logger.info("Creating DataSetsPerformance Table");
			String sqlCreateTableDataSetsPerformance = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getDataSetUsageStatsDBSchema()
				+ ".datasetsperformance(ds_type STRING,\n"
				+ "    period_end STRING,\n"
				+ "    period_from STRING,\n"
				+ "    access_method STRING,\n"
				+ "    metric_type STRING,\n"
				+ "    count INT,\n"
				+ "    country_counts STRING,\n"
				+ "    reportid STRING)\n"
				+ "    CLUSTERED BY (ds_type)\n"
				+ "	into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTableDataSetsPerformance);
			logger.info("DataSetsPerformance Table Created");

			stmt.close();
			ConnectDB.getHiveConnection().close();

		} catch (Exception e) {
			logger.error("Failed to create tables: " + e);
			throw new Exception("Failed to create tables: " + e.toString(), e);
		}
	}

	private Connection getConnection() throws SQLException {
		return ConnectDB.getHiveConnection();
	}
}
