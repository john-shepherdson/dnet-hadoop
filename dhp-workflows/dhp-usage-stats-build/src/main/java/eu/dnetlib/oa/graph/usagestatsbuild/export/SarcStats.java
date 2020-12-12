
package eu.dnetlib.oa.graph.usagestatsbuild.export;

import java.io.*;
// import java.io.BufferedReader;
// import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class SarcStats {

	private Statement stmtHive = null;
	private Statement stmtImpala = null;

	private static final Logger logger = LoggerFactory.getLogger(SarcStats.class);

	public SarcStats() throws Exception {
//		createTables();
	}

	private void createTables() throws Exception {
		try {

			stmtHive = ConnectDB.getHiveConnection().createStatement();
			String sqlCreateTableSushiLog = "CREATE TABLE IF NOT EXISTS sushilog(source TEXT, repository TEXT, rid TEXT, date TEXT, metric_type TEXT, count INT, PRIMARY KEY(source, repository, rid, date, metric_type));";
			stmtHive.executeUpdate(sqlCreateTableSushiLog);

			// String sqlCopyPublicSushiLog="INSERT INTO sushilog SELECT * FROM public.sushilog;";
			// stmt.executeUpdate(sqlCopyPublicSushiLog);
			String sqlcreateRuleSushiLog = "CREATE OR REPLACE RULE ignore_duplicate_inserts AS "
				+ " ON INSERT TO sushilog "
				+ " WHERE (EXISTS ( SELECT sushilog.source, sushilog.repository,"
				+ "sushilog.rid, sushilog.date "
				+ "FROM sushilog "
				+ "WHERE sushilog.source = new.source AND sushilog.repository = new.repository AND sushilog.rid = new.rid AND sushilog.date = new.date AND sushilog.metric_type = new.metric_type)) DO INSTEAD NOTHING;";
			stmtHive.executeUpdate(sqlcreateRuleSushiLog);
			String createSushiIndex = "create index if not exists sushilog_duplicates on sushilog(source, repository, rid, date, metric_type);";
			stmtHive.executeUpdate(createSushiIndex);

			stmtHive.close();
			ConnectDB.getHiveConnection().close();
			logger.info("Sushi Tables Created");
		} catch (Exception e) {
			logger.error("Failed to create tables: " + e);
			throw new Exception("Failed to create tables: " + e.toString(), e);
		}
	}

	public void processSarc() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Creating sarc_downloads_stats_tmp table");
		String createDownloadsStats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".sarc_downloads_stats_tmp "
			+ "(`source` string, "
			+ "`repository_id` string, "
			+ "`result_id` string, "
			+ "`date`	string, "
			+ "`count` bigint,	"
			+ "`openaire`	bigint)";
		stmt.executeUpdate(createDownloadsStats);
		logger.info("Created sarc_downloads_stats_tmp table");

		logger.info("Inserting into sarc_downloads_stats_tmp");
		String insertSarcStats = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".sarc_downloads_stats_tmp "
			+ "SELECT s.source, d.id AS repository_id, "
			+ "ro.id as result_id, CONCAT(CAST(YEAR(`date`) AS STRING), '/', "
			+ "LPAD(CAST(MONTH(`date`) AS STRING), 2, '0')) AS `date`, s.count, '0' "
			+ "FROM " + ConnectDB.getUsageRawDataDBSchema() + ".sushilog s, "
			+ ConnectDB.getStatsDBSchema() + ".datasource_oids d, "
			+ ConnectDB.getStatsDBSchema() + ".result_pids ro "
			+ "WHERE d.oid LIKE CONCAT('%', s.repository, '%') AND d.id like CONCAT('%', 'sarcservicod', '%') "
			+ "AND s.rid=ro.pid AND ro.type='Digital Object Identifier' AND s.metric_type='ft_total' AND s.source='SARC-OJS'";
		stmt.executeUpdate(insertSarcStats);
		logger.info("Inserted into sarc_downloads_stats_tmp");

		stmt.close();
		// ConnectDB.getHiveConnection().close();
	}

}
