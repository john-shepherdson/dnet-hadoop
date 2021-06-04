/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * @author D. Pierrakos
 */
/**
 * @author D. Pierrakos
 */
import com.mchange.v2.c3p0.ComboPooledDataSource;

public abstract class ConnectDB {

	public static Connection DB_HIVE_CONNECTION;
	public static Connection DB_IMPALA_CONNECTION;

	private static String dbHiveUrl;
	private static String dbImpalaUrl;
	private static String datasetUsageStatsDBSchema;
	private static String datasetsUsageStatsPermanentDBSchema;
	private static String statsDBSchema;
	private final static Logger logger = Logger.getLogger(ConnectDB.class);
	private Statement stmt = null;

	static void init() throws ClassNotFoundException {

		dbHiveUrl = ExecuteWorkflow.dbHiveUrl;
		dbImpalaUrl = ExecuteWorkflow.dbImpalaUrl;
		datasetUsageStatsDBSchema = ExecuteWorkflow.datasetUsageStatsDBSchema;
		datasetsUsageStatsPermanentDBSchema = ExecuteWorkflow.datasetsUsageStatsPermanentDBSchema;
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
		String datePattern = "YYYYMMdd";
		DateFormat df = new SimpleDateFormat(datePattern);
// Get the today date using Calendar object.
		Date today = Calendar.getInstance().getTime();
		String todayAsString = df.format(today);

		return ConnectDB.datasetUsageStatsDBSchema + "_" + todayAsString;
	}

	public static String getStatsDBSchema() {
		return ConnectDB.statsDBSchema;
	}

	public static String getDatasetsUsagestatsPermanentDBSchema() {
		return ConnectDB.datasetsUsageStatsPermanentDBSchema;
	}

	private static Connection connectHive() throws SQLException {
		logger.info("trying to open Hive connection...");

		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(dbHiveUrl);
		cpds.setUser("dimitris.pierrakos");
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

		logger.info("Opened HIVE successfully");

		return cpds.getConnection();
//		Connection connection = DriverManager.getConnection(dbHiveUrl);
//		logger.debug("Opened Hive successfully");
//
//		return connection;

	}

	private static Connection connectImpala() throws SQLException {
		logger.info("trying to open Impala connection...");
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(dbImpalaUrl);
		cpds.setUser("dimitris.pierrakos");
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

		logger.info("Opened Impala successfully");
		return cpds.getConnection();
//		Connection connection = DriverManager.getConnection(dbHiveUrl);
//		logger.debug("Opened Impala successfully");
//
//		return connection;

	}
}
