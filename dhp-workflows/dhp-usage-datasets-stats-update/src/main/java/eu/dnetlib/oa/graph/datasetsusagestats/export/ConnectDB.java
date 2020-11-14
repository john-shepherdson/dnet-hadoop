/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
/**
 * @author D. Pierrakos, S. Zoupanos
 */
import com.mchange.v2.c3p0.ComboPooledDataSource;

public abstract class ConnectDB {

	public static Connection DB_HIVE_CONNECTION;
	public static Connection DB_IMPALA_CONNECTION;

	private static String dbHiveUrl;
	private static String dbImpalaUrl;
	private static String datasetUsageStatsDBSchema;
	private static String statsDBSchema;
	private final static Logger logger = Logger.getLogger(ConnectDB.class);
        private Statement stmt = null;
        
	static void init() throws ClassNotFoundException {

		dbHiveUrl = ExecuteWorkflow.dbHiveUrl;
		dbImpalaUrl = ExecuteWorkflow.dbImpalaUrl;
		datasetUsageStatsDBSchema = ExecuteWorkflow.datasetUsageStatsDBSchema;
		statsDBSchema = ExecuteWorkflow.statsDBSchema;

		Class.forName("org.apache.hive.jdbc.HiveDriver");
	}

	public static Connection getHiveConnection() throws SQLException {
		if (DB_HIVE_CONNECTION != null && !DB_HIVE_CONNECTION.isClosed()) {
			return DB_HIVE_CONNECTION;
		} else {
			DB_HIVE_CONNECTION = connectHive();

			return DB_HIVE_CONNECTION;
		}
	}

	public static Connection getImpalaConnection() throws SQLException {
		if (DB_IMPALA_CONNECTION != null && !DB_IMPALA_CONNECTION.isClosed()) {
			return DB_IMPALA_CONNECTION;
		} else {
			DB_IMPALA_CONNECTION = connectImpala();

			return DB_IMPALA_CONNECTION;
		}
	}

	public static String getDataSetUsageStatsDBSchema() {
		return ConnectDB.datasetUsageStatsDBSchema;
	}

	public static String getStatsDBSchema() {
		return ConnectDB.statsDBSchema;
	}

	private static Connection connectHive() throws SQLException {
		/*
		 * Connection connection = DriverManager.getConnection(dbHiveUrl); Statement stmt =
		 * connection.createStatement(); log.debug("Opened database successfully"); return connection;
		 */
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(dbHiveUrl);
		cpds.setAcquireIncrement(1);
		cpds.setMaxPoolSize(100);
		cpds.setMinPoolSize(1);
		cpds.setInitialPoolSize(1);
		cpds.setMaxIdleTime(300);
		cpds.setMaxConnectionAge(36000);

		cpds.setAcquireRetryAttempts(5);
		cpds.setAcquireRetryDelay(2000);
		cpds.setBreakAfterAcquireFailure(false);

		cpds.setCheckoutTimeout(0);
		cpds.setPreferredTestQuery("SELECT 1");
		cpds.setIdleConnectionTestPeriod(60);
                
                logger.info("Opened database successfully");

                return cpds.getConnection();

	}

	private static Connection connectImpala() throws SQLException {
		/*
		 * Connection connection = DriverManager.getConnection(dbImpalaUrl); Statement stmt =
		 * connection.createStatement(); log.debug("Opened database successfully"); return connection;
		 */
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(dbImpalaUrl);
		cpds.setAcquireIncrement(1);
		cpds.setMaxPoolSize(100);
		cpds.setMinPoolSize(1);
		cpds.setInitialPoolSize(1);
		cpds.setMaxIdleTime(300);
		cpds.setMaxConnectionAge(36000);

		cpds.setAcquireRetryAttempts(5);
		cpds.setAcquireRetryDelay(2000);
		cpds.setBreakAfterAcquireFailure(false);

		cpds.setCheckoutTimeout(0);
		cpds.setPreferredTestQuery("SELECT 1");
		cpds.setIdleConnectionTestPeriod(60);

                logger.info("Opened database successfully");
		return cpds.getConnection();

	}

	private void createDatabase() throws Exception {
		try {
			stmt = ConnectDB.getHiveConnection().createStatement();

			logger.info("Dropping logs DB: " + ConnectDB.getDataSetUsageStatsDBSchema());
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

			// Create Piwiklog table - This table should exist
			String sqlCreateTablePiwikLog = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getDataSetUsageStatsDBSchema()
				+ ".piwiklog(source INT, id_visit STRING, country STRING, action STRING, url STRING, "
				+ "entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) "
				+ "clustered by (source, id_visit, action, timestamp, entity_id) "
				+ "into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTablePiwikLog);

			/////////////////////////////////////////
			// Rule for duplicate inserts @ piwiklog
			/////////////////////////////////////////

			String sqlCreateTablePortalLog = "CREATE TABLE IF NOT EXISTS "
				+ ConnectDB.getDataSetUsageStatsDBSchema()
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
}
/*
CREATE TABLE IF NOT EXISTS dataciteReports (reportid STRING, 
	name STRING, 
    source STRING,
    release STRING,
    createdby STRING,
    report_end_date STRING,
    report_start_date STRING)
    CLUSTERED BY (reportid)
	into 100 buckets stored as orc tblproperties('transactional'='true');
*/