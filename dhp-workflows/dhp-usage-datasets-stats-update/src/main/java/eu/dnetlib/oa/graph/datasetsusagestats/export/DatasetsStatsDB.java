
package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos
 */
public class DatasetsStatsDB {

	private String logPath;
	private String logRepoPath;
	private String logPortalPath;

	private Statement stmt = null;

	private static final Logger logger = LoggerFactory.getLogger(DatasetsStatsDB.class);

	public DatasetsStatsDB(String logRepoPath, String logPortalPath) throws Exception {
		this.logRepoPath = logRepoPath;
		this.logPortalPath = logPortalPath;

	}

	public void recreateDBAndTables() throws Exception {
		this.createDatabase();
		this.createTables();
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

			// Create Datasets Performance Table
			logger.info("Creating DataSetsPerformance Table");
			String sqlCreateTableDataSetsPerformance = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getDataSetUsageStatsDBSchema()
				+ ".datasetsperformance(ds_type STRING,\n"
				+ " ds_title STRING,\n"
				+ " yop STRING,\n"
				+ " dataset_type STRING, \n"
				+ " uri STRING,\n"
				+ " platform STRING,\n"
				+ " publisher STRING,\n"
				+ " publisher_id array<struct<type:STRING, value:STRING>>,\n"
				+ " dataset_contributors array<struct<type:STRING, value:STRING>>,\n"
				+ " period_end STRING,\n"
				+ " period_from STRING,\n"
				+ " access_method STRING,\n"
				+ " metric_type STRING,\n"
				+ " count INT,\n"
				+ " reportid STRING)\n"
				+ " CLUSTERED BY (ds_type)\n"
				+ " into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTableDataSetsPerformance);
			logger.info("DataSetsPerformance Table Created");

			stmt.close();
			ConnectDB.getHiveConnection().close();

		} catch (Exception e) {
			logger.error("Failed to create tables: " + e);
			throw new Exception("Failed to create tables: " + e.toString(), e);
		}
	}

}
