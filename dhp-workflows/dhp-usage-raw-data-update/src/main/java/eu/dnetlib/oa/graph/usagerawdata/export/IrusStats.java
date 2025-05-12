
package eu.dnetlib.oa.graph.usagerawdata.export;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IrusStats {

	private String irusUKURL;

	private static final Logger logger = LoggerFactory.getLogger(IrusStats.class);
	private static final DateTimeFormatter YYYY_MM_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM");
	private static final DateTimeFormatter YYYY_MM_DD_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	public IrusStats(String irusUKURL) {
		this.irusUKURL = irusUKURL;
	}

	public void reCreateLogDirs() throws Exception {
		FileSystem dfs = FileSystem.get(new Configuration());

		logger.info("Deleting irusUKReport directory: " + ExecuteWorkflow.irusUKReportPath);
		dfs.delete(new Path(ExecuteWorkflow.irusUKReportPath), true);

		logger.info("Creating irusUKReport directory: " + ExecuteWorkflow.irusUKReportPath);
		dfs.mkdirs(new Path(ExecuteWorkflow.irusUKReportPath));
	}

	public void createTables() throws Exception {
		try {
			logger.info("Creating sushilog");
			Statement stmt = ConnectDB.getHiveConnection().createStatement();
			String sqlCreateTableSushiLog = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
				+ ".sushilog(source STRING, "
				+ "repository STRING, rid STRING, date STRING, metric_type STRING, count INT)  clustered by (source, "
				+ "repository, rid, date, metric_type) into 100 buckets stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTableSushiLog);
			logger.info("Created sushilog");

			stmt.close();
			ConnectDB.getHiveConnection().close();
			logger.info("Sushi Tables Created");
		} catch (Exception e) {
			logger.error("Failed to create tables: " + e);
			throw new Exception("Failed to create tables: " + e.toString(), e);
		}
	}

	public void processIrusStats() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Adding JSON Serde jar");
		stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
		logger.info("Added JSON Serde jar");

		logger.info("Dropping sushilogtmp_json table");
		String dropSushilogtmpJson = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".sushilogtmp_json";
		stmt.executeUpdate(dropSushilogtmpJson);
		logger.info("Dropped sushilogtmp_json table");

		logger.info("Creating irus_sushilogtmp_json table");
		String createSushilogtmpJson = "CREATE EXTERNAL TABLE IF NOT EXISTS "
			+ ConnectDB.getUsageStatsDBSchema() + ".irus_sushilogtmp_json(\n"
			+ "	`ItemIdentifier` ARRAY<\n"
			+ "						struct<\n"
			+ "							Type: STRING,\n"
			+ "							Value: STRING\n"
			+ "							>\n"
			+ "						>,\n"
			+ "	`ItemPerformance` ARRAY<\n"
			+ "						struct<\n"
			+ "							`Period`:  struct<\n"
			+ "										`Begin`: STRING,\n"
			+ "										`End`: STRING\n"
			+ "										>,\n"
			+ "							`Instance`:  struct<\n"
			+ "										`Count`: STRING,\n"
			+ "										`MetricType`: STRING\n"
			+ "										>\n"
			+ "							>\n"
			+ "						>\n"
			+ ")\n"
			+ "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n"
			+ "LOCATION '" + ExecuteWorkflow.irusUKReportPath + "'\n"
			+ "TBLPROPERTIES (\"transactional\"=\"false\")";
		stmt.executeUpdate(createSushilogtmpJson);
		logger.info("Created irus_sushilogtmp_json table");

		logger.info("Dropping irus_sushilogtmp table");
		String dropSushilogtmp = "DROP TABLE IF EXISTS "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".irus_sushilogtmp";
		stmt.executeUpdate(dropSushilogtmp);
		logger.info("Dropped irus_sushilogtmp table");

		logger.info("Creating irus_sushilogtmp table");
		String createSushilogtmp = "CREATE TABLE " + ConnectDB.getUsageStatsDBSchema()
			+ ".irus_sushilogtmp(source STRING, repository STRING, "
			+ "rid STRING, date STRING, metric_type STRING, count INT) clustered by (source) into 100 buckets stored as orc "
			+ "tblproperties('transactional'='true')";
		stmt.executeUpdate(createSushilogtmp);
		logger.info("Created irus_sushilogtmp table");

		logger.info("Inserting to irus_sushilogtmp table");
		String insertSushilogtmp = "INSERT INTO  " + ConnectDB.getUsageStatsDBSchema() + ".irus_sushilogtmp "
			+ "SELECT 'IRUS-UK', CONCAT('opendoar____::', split(split(INPUT__FILE__NAME,'IrusIRReport_')[1],'_')[0]), "
			+ "`ItemIdent`.`Value`, `ItemPerf`.`Period`.`Begin`, "
			+ "`ItemPerf`.`Instance`.`MetricType`, `ItemPerf`.`Instance`.`Count` "
			+ "FROM " + ConnectDB.getUsageStatsDBSchema() + ".irus_sushilogtmp_json "
			+ "LATERAL VIEW posexplode(ItemIdentifier) ItemIdentifierTable AS seqi, ItemIdent "
			+ "LATERAL VIEW posexplode(ItemPerformance) ItemPerformanceTable AS seqp, ItemPerf "
			+ "WHERE `ItemIdent`.`Type`= 'OAI'";
		stmt.executeUpdate(insertSushilogtmp);
		logger.info("Inserted to irus_sushilogtmp table");

		logger.info("Inserting to sushilog table");
		String insertToShushilog = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".sushilog SELECT * FROM "
			+ ConnectDB.getUsageStatsDBSchema()
			+ ".irus_sushilogtmp";
		stmt.executeUpdate(insertToShushilog);
		logger.info("Inserted to sushilog table");

		ConnectDB.getHiveConnection().close();
	}

	public void getIrusRRReport(String irusUKReportPath) throws Exception {
		// Setting the starting period
		LocalDate start = ExecuteWorkflow.startingLogPeriod;

		// Setting the ending period (last day of the month)
		LocalDate end = LocalDate.now().minusDays(1);

		logger.info("(getIrusRRReport) Starting period for log download: {}", YYYY_MM_FORMAT.format(start));
		logger.info("(getIrusRRReport) Ending period for log download: {}", YYYY_MM_FORMAT.format(end));

		String reportUrl = irusUKURL + "GetReport/?Report=RR1&Release=4&RequestorID=OpenAIRE&BeginDate="
			+ YYYY_MM_FORMAT.format(start) + "&EndDate=" + YYYY_MM_FORMAT.format(end)
			+ "&RepositoryIdentifier=&ItemDataType=&NewJiscBand=&Granularity=Monthly&Callback=";
		logger.info("(getIrusRRReport) Getting report: {}", reportUrl);

		String text = getJson(reportUrl, "", "");

		List<String> opendoarsToVisit = new ArrayList<String>();
		JSONParser parser = new JSONParser();
		JSONObject jsonObject = (JSONObject) parser.parse(text);
		jsonObject = (JSONObject) jsonObject.get("ReportResponse");
		jsonObject = (JSONObject) jsonObject.get("Report");
		jsonObject = (JSONObject) jsonObject.get("Report");
		jsonObject = (JSONObject) jsonObject.get("Customer");
		JSONArray jsonArray = (JSONArray) jsonObject.get("ReportItems");
		if (jsonArray != null) {
			int i = 0;
			for (Object aJsonArray : jsonArray) {
				JSONObject jsonObjectRow = (JSONObject) aJsonArray;
				JSONArray itemIdentifier = (JSONArray) jsonObjectRow.get("ItemIdentifier");
				for (Object identifier : itemIdentifier) {
					JSONObject opendoar = (JSONObject) identifier;
					if (opendoar.get("Type").toString().equals("OpenDOAR")) {
						i++;
						opendoarsToVisit.add(opendoar.get("Value").toString());
						break;
					}
				}
				// break;
			}

			logger.info("(getIrusRRReport) Found the following opendoars for download: {}", opendoarsToVisit);

			if (ExecuteWorkflow.irusNumberOfOpendoarsToDownload > 0
				&& ExecuteWorkflow.irusNumberOfOpendoarsToDownload <= opendoarsToVisit.size()) {
				logger.info("Trimming siteIds list to the size of: " + ExecuteWorkflow.irusNumberOfOpendoarsToDownload);
				opendoarsToVisit = opendoarsToVisit.subList(0, ExecuteWorkflow.irusNumberOfOpendoarsToDownload);
			}

			logger.info("(getIrusRRReport) Downloading the followins opendoars: {}", opendoarsToVisit);

			for (String opendoar : opendoarsToVisit) {
				logger.info("Now working on openDoar: {}", opendoar);
				this.getIrusIRReport(opendoar, irusUKReportPath);
			}
			logger.info("(getIrusRRReport) Finished with report: {}", reportUrl);
		} else {
			logger.info("IRUS Reports not found for day");
		}

	}

	private void getIrusIRReport(String opendoar, String irusUKReportPath) throws Exception {
		logger.info("(getIrusIRReport) Getting report(s) with opendoar: {}", opendoar);

		ConnectDB.getHiveConnection().setAutoCommit(false);
		LocalDate start = ExecuteWorkflow.startingLogPeriod;
		logger.info("(getIrusIRReport) Starting period for log download: {}", YYYY_MM_FORMAT.format(start));

		LocalDate end = LocalDate.now().minusDays(1);
		logger.info("(getIrusIRReport) Ending period for log download: {}", YYYY_MM_FORMAT.format(end));

		PreparedStatement st = ConnectDB
			.getHiveConnection()
			.prepareStatement(
				"SELECT max(date) FROM " + ConnectDB.getUsageStatsDBSchema() + ".sushilog WHERE repository=?");
		st.setString(1, "opendoar____::" + opendoar);
		ResultSet rs_date = st.executeQuery();

		LocalDate dateMax = null;
		while (rs_date.next()) {
			String dateStr = rs_date.getString(1);
			if (dateStr != null && !"null".equals(dateStr) && !"".equals(dateStr)) {
				dateMax = LocalDate.parse(dateStr, YYYY_MM_DD_FORMAT);
				start = dateMax;
			}
		}
		rs_date.close();

		if (dateMax != null && !end.isAfter(dateMax)) {
			logger.info("Date found in logs {} and not downloading logs for {}", dateMax, opendoar);
			return;
		}
		start = start.plusMonths(1);
		while (start.isBefore(end)) {
			String reportUrl = this.irusUKURL + "GetReport/?Report=IR1&Release=4&RequestorID=OpenAIRE&BeginDate="
				+ YYYY_MM_FORMAT.format(start) + "&EndDate=" + YYYY_MM_FORMAT.format(start)
				+ "&RepositoryIdentifier=opendoar%3A" + opendoar
				+ "&ItemIdentifier=&ItemDataType=&hasDOI=&Granularity=Monthly&Callback=";
			start = start.plusMonths(1);
			String text = getJson(reportUrl, "", "");
			if (text == null)
				continue;
			FileSystem fs = FileSystem.get(new Configuration());
			String filePath = irusUKReportPath + "/" + "IrusIRReport_" + opendoar + "_" + YYYY_MM_FORMAT.format(start)
				+ ".json";
			logger.info("Storing to file: {}", filePath);

			FSDataOutputStream fin = fs.create(new Path(filePath), true);

			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject) parser.parse(text);
			jsonObject = (JSONObject) jsonObject.get("ReportResponse");
			jsonObject = (JSONObject) jsonObject.get("Report");
			jsonObject = (JSONObject) jsonObject.get("Report");
			jsonObject = (JSONObject) jsonObject.get("Customer");
			JSONArray jsonArray = (JSONArray) jsonObject.get("ReportItems");
			if (jsonArray == null)
				continue;
			for (Object aJsonArray : jsonArray) {
				JSONObject jsonObjectRow = (JSONObject) aJsonArray;
				fin.write(jsonObjectRow.toJSONString().getBytes());
				fin.writeChar('\n');
			}
			fin.close();
		}
		logger.info("(getIrusIRReport) Finished downloading report(s) with opendoar: {}", opendoar);
	}

	private String getJson(String url, String username, String password) {
		try {
			URL website = new URL(url);
			URLConnection connection = website.openConnection();
			// connection.setRequestProperty ("Authorization", "Basic "+encoded);
			StringBuilder response;
			try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				response = new StringBuilder();
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
					response.append("\n");
				}
			}
			return response.toString();
		} catch (Exception e) {
			logger.error("IrusStats - Failed to get URL", e);
			return null;
		}
	}
}
