
package eu.dnetlib.oa.graph.usagerawdata.export;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
public class PiwikStatsDB_B2SHARE {

	private String logPath;
	private String logRepoPath;
	private String logPortalPath;

	private Statement stmt = null;

	private static final Logger logger = LoggerFactory.getLogger(PiwikStatsDB_B2SHARE.class);

	private String CounterRobotsURL;
	private ArrayList robotsList;

	public PiwikStatsDB_B2SHARE(String logRepoPath, String logPortalPath) throws Exception {
		this.logRepoPath = logRepoPath;
		this.logPortalPath = logPortalPath;

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

	public void processB2SHARELogs() throws Exception {
		try {

			logger.info("Processing B2SHARE logs");
			processLog();
			logger.info("B2SHARE logs process done");

			logger.info("Removing double clicks from B2SHARE logs");
			removeDoubleClicks();
			logger.info("Removing double clicks from B2SHARE logs done");

			logger.info("Updating Production Tables");
			updateProdTables();
			logger.info("Updated Production Tables");

		} catch (Exception e) {
			logger.error("Failed to process logs: " + e);
			throw new Exception("Failed to process logs: " + e.toString(), e);
		}
	}

	public void processLog() throws Exception {

		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Adding JSON Serde jar");
		stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
		logger.info("Added JSON Serde jar");

		logger.info("Dropping piwiklog_b2share_tmp_json table");
		String drop_piwiklogtmp_json = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklog_b2share_tmp_json";
		stmt.executeUpdate(drop_piwiklogtmp_json);
		logger.info("Dropped piwiklog_b2share_tmp_json table");

		logger.info("Creating piwiklog_b2share_tmp_json");
		String create_piwiklogtmp_json = "CREATE EXTERNAL TABLE IF NOT EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklog_b2share_tmp_json(\n"
			+ "	`idSite` STRING,\n"
			+ "	`idVisit` STRING,\n"
			+ "	`country` STRING,\n"
			+ "	`referrerName` STRING,\n"
			+ "	`browser` STRING,\n"
			+ "	`actionDetails` ARRAY<\n"
			+ "						struct<\n"
			+ "							type: STRING,\n"
			+ "							url: STRING,\n"
			+ "							eventAction: STRING,\n"
			+ "							eventName: STRING,\n"
			+ "							timestamp: String\n"
			+ "							>\n"
			+ "						>\n"
			+ ")\n"
			+ "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n"
			+ "LOCATION '" + ExecuteWorkflow.repoLogPath + "'\n"
			+ "TBLPROPERTIES (\"transactional\"=\"false\")";
		stmt.executeUpdate(create_piwiklogtmp_json);
		logger.info("Created piwiklog_b2share_tmp_json");

		logger.info("Dropping piwiklogtmp table");
		String drop_piwiklogtmp = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklogtmp";
		stmt.executeUpdate(drop_piwiklogtmp);
		logger.info("Dropped piwiklogtmp");

		logger.info("Creating piwiklogb2sharetmp");
		String create_piwiklogtmp = "CREATE TABLE "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklogb2sharetmp (source BIGINT, id_Visit STRING, country STRING, action STRING, url STRING, "
			+ "entity_id STRING, source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING)  "
			+ "clustered by (source) into 100 buckets stored as orc tblproperties('transactional'='true')";
		stmt.executeUpdate(create_piwiklogtmp);
		logger.info("Created piwiklogb2sharetmp");

		logger.info("Inserting into piwiklogb2sharetmp");
		String insert_piwiklogtmp = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp "
			+ "SELECT DISTINCT cast(idSite as BIGINT) as source, idVisit  as id_Visit, country, "
			+ "actiondetail.eventAction as action, actiondetail.url as url, "
			+ "actiondetail.eventName as entity_id, "
			+ "'repItem' as source_item_type, from_unixtime(cast(actiondetail.timestamp as BIGINT)) as timestamp, "
			+ "referrerName as referrer_name, browser as agent\n"
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklog_b2share_tmp_json\n"
			+ "LATERAL VIEW explode(actiondetails) actiondetailsTable AS actiondetail";
		stmt.executeUpdate(insert_piwiklogtmp);
		logger.info("Inserted into piwiklogb2sharetmp");

		stmt.close();
	}

	public void removeDoubleClicks() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Cleaning download double clicks");
		// clean download double clicks
		String sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp "
			+ "WHERE EXISTS (\n"
			+ "SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp \n"
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp p1, "
			+ ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp p2\n"
			+ "WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id \n"
			+ "AND p1.action=p2.action AND p1.action='download' AND p1.timestamp!=p2.timestamp \n"
			+ "AND p1.timestamp<p2.timestamp AND ((unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))/60)<30 \n"
			+ "AND piwiklogb2sharetmp.source=p1.source AND piwiklogb2sharetmp.id_visit=p1.id_visit \n"
			+ "AND piwiklogb2sharetmp.action=p1.action AND piwiklogb2sharetmp.entity_id=p1.entity_id AND piwiklogb2sharetmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		logger.info("Cleaned download double clicks");

		// clean view double clicks
		logger.info("Cleaning action double clicks");
		sql = "DELETE from " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp "
			+ "WHERE EXISTS (\n"
			+ "SELECT DISTINCT p1.source, p1.id_visit, p1.action, p1.entity_id, p1.timestamp \n"
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp p1, "
			+ ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp p2\n"
			+ "WHERE p1.source=p2.source AND p1.id_visit=p2.id_visit AND p1.entity_id=p2.entity_id \n"
			+ "AND p1.action=p2.action AND p1.action='action' AND p1.timestamp!=p2.timestamp \n"
			+ "AND p1.timestamp<p2.timestamp AND (unix_timestamp(p2.timestamp)-unix_timestamp(p1.timestamp))<10 \n"
			+ "AND piwiklogb2sharetmp.source=p1.source AND piwiklogb2sharetmp.id_visit=p1.id_visit \n"
			+ "AND piwiklogb2sharetmp.action=p1.action AND piwiklogb2sharetmp.entity_id=p1.entity_id AND piwiklogb2sharetmp.timestamp=p1.timestamp)";
		stmt.executeUpdate(sql);
		logger.info("Cleaned action double clicks");
		stmt.close();
	}

	private void updateProdTables() throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Updating recordview to action piwiklog");
		String sqlUpdateAction = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp "
			+ "set action='action' where action='recordview'";
		stmt.executeUpdate(sqlUpdateAction);

		logger.info("Updating fileDownload to download piwiklog");
		String sqlUpdateDownload = "UPDATE " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp "
			+ "set action='download' where action='filedownload'";
		stmt.executeUpdate(sqlUpdateDownload);

		logger.info("Inserting B2SHARE data to piwiklog");
		String sql = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".piwiklog "
			+ "SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".piwiklogb2sharetmp";
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

		logger.info("Creating Datasets Views Table");
		String sql = "Create TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".datacite_views as select * from datasetsusagestats_20210301.datacite_views";
		stmt.executeUpdate(sql);
		logger.info("Datasets Views Table created");

		logger.info("Creating Datasets Downloads Table");
		sql = "Create TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".datacite_downloads as select * from datasetsusagestats_20210301.datacite_downloads";
		stmt.executeUpdate(sql);
		logger.info("Datasets Downloads Table created");

	}
}
