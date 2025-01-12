
package eu.dnetlib.oa.graph.usagerawdata.export;

import java.io.*;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class LaReferenciaStats {

	private static final Logger logger = LoggerFactory.getLogger(LaReferenciaStats.class);

	private String logRepoPath;

	private Statement stmt = null;

	private String CounterRobotsURL;
	private ArrayList robotsList;

	public LaReferenciaStats(String logRepoPath) throws Exception {
		this.logRepoPath = logRepoPath;
		this.createTables();
//		this.createTmpTables();
	}

	/*
	 * private void connectDB() throws Exception { try { ConnectDB connectDB = new ConnectDB(); } catch (Exception e) {
	 * log.error("Connect to db failed: " + e); throw new Exception("Failed to connect to db: " + e.toString(), e); } }
	 */
	private void createTables() throws Exception {
		try {
			Statement stmt = ConnectDB.getHiveConnection().createStatement();

			logger.info("Creating LaReferencia tables");
			String sqlCreateTableLareferenciaLog = "CREATE TABLE IF NOT EXISTS " +
				ConnectDB.getUsageStatsDBSchema() + ".lareferencialog(matomoid INT, " +
				"source STRING, id_visit STRING, country STRING, action STRING, url STRING, entity_id STRING, " +
				"source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) " +
				"clustered by (source, id_visit, action, timestamp, entity_id) into 100 buckets " +
				"stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTableLareferenciaLog);
			logger.info("Created LaReferencia tables");

			stmt.close();
			ConnectDB.getHiveConnection().close();
			logger.info("Lareferencia Tables Created");

		} catch (Exception e) {
			logger.error("Failed to create tables: " + e);
			throw new Exception("Failed to create tables: " + e.toString(), e);
			// System.exit(0);
		}
	}

	public void processLogs() throws Exception {
		try {
			logger.info("Processing LaReferencia repository logs");
			processlaReferenciaLog();
			logger.info("LaReferencia repository logs process done");

			logger.info("LaReferencia removing double clicks");
			removeDoubleClicks();
			logger.info("LaReferencia removed double clicks");

			logger.info("LaReferencia updating Production Tables");
			updateProdTables();
			logger.info("LaReferencia updated Production Tables");

		} catch (Exception e) {
			logger.error("Failed to process logs: " + e);
			throw new Exception("Failed to process logs: " + e.toString(), e);
		}
	}

	public void processlaReferenciaLog() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Adding JSON Serde jar");
		stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
		logger.info("Added JSON Serde jar");

		logger.info("Dropping lareferencialogtmp_json table");
		String drop_lareferencialogtmp_json = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".lareferencialogtmp_json";
		stmt.executeUpdate(drop_lareferencialogtmp_json);
		logger.info("Dropped lareferencialogtmp_json table");

		logger.info("Creating lareferencialogtmp_json");
		String create_lareferencialogtmp_json = "CREATE EXTERNAL TABLE IF NOT EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".lareferencialogtmp_json(\n" +
			"	`idSite` STRING,\n" +
			"	`idVisit` STRING,\n" +
			"	`country` STRING,\n" +
			"	`referrerName` STRING,\n" +
			"	`browser` STRING,\n" +
			"	`repItem` STRING,\n" +
			"	`actionDetails` ARRAY<\n" +
			"						struct<\n" +
			"							timestamp: STRING,\n" +
			"							type: STRING,\n" +
			"							url: STRING,\n" +
			"							`customVariables`: struct<\n" +
			"								`1`: struct<\n" +
			"								`customVariablePageValue1`: STRING\n" +
			"										>,\n" +
			"								`2`: struct<\n" +
			"								`customVariablePageValue2`: STRING\n" +
			"										>\n" +
			"								>\n" +
			"							>\n" +
			"						>" +
			")\n" +
			"ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
			"LOCATION '" + ExecuteWorkflow.lareferenciaLogPath + "'\n" +
			"TBLPROPERTIES (\"transactional\"=\"false\")";
		stmt.executeUpdate(create_lareferencialogtmp_json);
		logger.info("Created lareferencialogtmp_json");

		logger.info("Dropping lareferencialogtmp table");
		String drop_lareferencialogtmp = "DROP TABLE IF EXISTS " +
			ConnectDB.getUsageStatsDBSchema() +
			".lareferencialogtmp";
		stmt.executeUpdate(drop_lareferencialogtmp);
		logger.info("Dropped lareferencialogtmp table");

		logger.info("Creating lareferencialogtmp");
		String create_lareferencialogtmp = "CREATE TABLE " +
			ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp(matomoid INT, " +
			"source STRING, id_visit STRING, country STRING, action STRING, url STRING, entity_id STRING, " +
			"source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) " +
			"clustered by (source, id_visit, action, timestamp, entity_id) into 100 buckets " +
			"stored as orc tblproperties('transactional'='true')";
		stmt.executeUpdate(create_lareferencialogtmp);
		logger.info("Created lareferencialogtmp");

		logger.info("Inserting into lareferencialogtmp");
		String insert_lareferencialogtmp = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp " +
			"SELECT DISTINCT cast(idSite as INT) as matomoid, CONCAT('opendoar____::', " +
			"actiondetail.customVariables.`2`.customVariablePageValue2) as source, idVisit  as id_Visit, country, " +
			"actiondetail.type as action, actiondetail.url as url, " +
			"actiondetail.customVariables.`1`.`customVariablePageValue1` as entity_id, " +
			"'repItem' as source_item_type, from_unixtime(cast(actiondetail.timestamp as BIGINT)) as timestamp, " +
			"referrerName as referrer_name, browser as agent " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp_json " +
			"LATERAL VIEW explode(actiondetails) actiondetailsTable AS actiondetail";
		stmt.executeUpdate(insert_lareferencialogtmp);
		logger.info("Inserted into lareferencialogtmp");

		stmt.close();
	}

	public void removeDoubleClicks() throws Exception {

		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Cleaning download double clicks");
		// clean download double clicks
		String sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp WHERE EXISTS (" +
			"SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp p1, " +
			ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp p2 " +
			"WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id " +
			"AND p1.action=p2.action AND p1.action='download' AND p1.timestamp!=p2.timestamp " +
			"AND p1.timestamp<p2.timestamp AND ((unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))/60)<30 " +
			"AND lareferencialogtmp.source=p1.source AND lareferencialogtmp.id_visit=p1.id_visit " +
			"AND lareferencialogtmp.action=p1.action AND lareferencialogtmp.entity_id=p1.entity_id " +
			"AND lareferencialogtmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		stmt.close();
		logger.info("Cleaned download double clicks");

		stmt = ConnectDB.getHiveConnection().createStatement();
		logger.info("Cleaning action double clicks");
		// clean view double clicks
		sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp WHERE EXISTS (" +
			"SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp " +
			"FROM " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp p1, " +
			ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp p2 " +
			"WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id " +
			"AND p1.action=p2.action AND p1.action='action' AND p1.timestamp!=p2.timestamp " +
			"AND p1.timestamp<p2.timestamp AND ((unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))/60)<10 " +
			"AND lareferencialogtmp.source=p1.source AND lareferencialogtmp.id_visit=p1.id_visit " +
			"AND lareferencialogtmp.action=p1.action AND lareferencialogtmp.entity_id=p1.entity_id " +
			"AND lareferencialogtmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		stmt.close();
		logger.info("Cleaned action double clicks");
		// conn.close();
	}

	private void updateProdTables() throws SQLException, Exception {

		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Updating lareferencialog");
		String sql = "insert into " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialog " +
			"select * from " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp";
		stmt.executeUpdate(sql);

		logger.info("Dropping lareferencialogtmp");
		sql = "DROP TABLE " + ConnectDB.getUsageStatsDBSchema() + ".lareferencialogtmp";
		logger.info("Dropped lareferencialogtmp");
		stmt.executeUpdate(sql);

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
				// log.info("Found hdfs file " + fileName);
				fileNames.add(fileName);
			}
			// hdfs.close();
		} catch (Exception e) {
			logger.error("HDFS file path with exported data does not exist : " + new Path(hdfs.getUri() + logRepoPath));
			throw new Exception("HDFS file path with exported data does not exist :   " + logRepoPath, e);
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

}
