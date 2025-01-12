/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.usagestatsbuild.export;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
import com.mchange.v2.c3p0.ComboPooledDataSource;

public abstract class ConnectDB {

	public static Connection DB_HIVE_CONNECTION;
	public static Connection DB_IMPALA_CONNECTION;

	private static String dbHiveUrl;
	private static String dbImpalaUrl;
	private static String usageRawDataDBSchema;
	private static String usageStatsDBSchema;
	private static String usagestatsPermanentDBSchema;
	private static String statsDBSchema;

	private ConnectDB() {
	}

	static void init() throws ClassNotFoundException {

		dbHiveUrl = ExecuteWorkflow.dbHiveUrl;
		dbImpalaUrl = ExecuteWorkflow.dbImpalaUrl;
		usageStatsDBSchema = ExecuteWorkflow.usageStatsDBSchema;
		statsDBSchema = ExecuteWorkflow.statsDBSchema;
		usageRawDataDBSchema = ExecuteWorkflow.usageRawDataDBSchema;
		usagestatsPermanentDBSchema = ExecuteWorkflow.usagestatsPermanentDBSchema;

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

	public static String getUsageRawDataDBSchema() {
		return ConnectDB.usageRawDataDBSchema;
	}

	public static String getUsageStatsDBSchema() {
		String datePattern = "YYYYMMdd";
		DateFormat df = new SimpleDateFormat(datePattern);
// Get the today date using Calendar object.
		Date today = Calendar.getInstance().getTime();
		String todayAsString = df.format(today);

		return ConnectDB.usageStatsDBSchema + "_" + todayAsString;
	}

	public static String getStatsDBSchema() {
		return ConnectDB.statsDBSchema;
	}

	public static String getUsagestatsPermanentDBSchema() {
		return ConnectDB.usagestatsPermanentDBSchema;
	}

	private static Connection connectHive() throws SQLException {
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(dbHiveUrl);
		cpds.setAcquireIncrement(1);
		cpds.setMaxPoolSize(100);
		cpds.setMinPoolSize(1);
		cpds.setInitialPoolSize(1);
		cpds.setMaxIdleTime(300);
		cpds.setMaxConnectionAge(36000);

		cpds.setAcquireRetryAttempts(30);
		cpds.setAcquireRetryDelay(2000);
		cpds.setBreakAfterAcquireFailure(false);

		cpds.setCheckoutTimeout(0);
		cpds.setPreferredTestQuery("SELECT 1");
		cpds.setIdleConnectionTestPeriod(60);
		return cpds.getConnection();

	}

	private static Connection connectImpala() throws SQLException {
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(dbImpalaUrl);
		cpds.setAcquireIncrement(1);
		cpds.setMaxPoolSize(100);
		cpds.setMinPoolSize(1);
		cpds.setInitialPoolSize(1);
		cpds.setMaxIdleTime(300);
		cpds.setMaxConnectionAge(36000);

		cpds.setAcquireRetryAttempts(30);
		cpds.setAcquireRetryDelay(2000);
		cpds.setBreakAfterAcquireFailure(false);

		cpds.setCheckoutTimeout(0);
		cpds.setPreferredTestQuery("SELECT 1");
		cpds.setIdleConnectionTestPeriod(60);

		return cpds.getConnection();

	}

}
