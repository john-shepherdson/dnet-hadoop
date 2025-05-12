
package eu.dnetlib.oa.graph.usagerawdata.export;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
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

public class LaReferenciaDownloadLogs {

	private final String piwikUrl;
	private final String tokenAuth;

	private final String APImethod = "?module=API&method=Live.getLastVisitsDetails";
	private final String format = "&format=json";
	private final String ApimethodGetAllSites = "?module=API&method=SitesManager.getSitesWithViewAccess";

	private static final DateTimeFormatter YYYY_MM_DD_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private static final DateTimeFormatter YYYY_MM_DD_HH_mm_ss = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	private static final Logger logger = LoggerFactory.getLogger(LaReferenciaDownloadLogs.class);

	// Get start/end period
	private LocalDate start;
	private LocalDate end;

	private static final int NUM_OF_DAYS = 30; // number of days to process since the start date

	public LaReferenciaDownloadLogs(String piwikUrl, String tokenAuth) throws Exception {
		this.piwikUrl = piwikUrl;
		this.tokenAuth = tokenAuth;
		this.createTables();
	}

	public void reCreateLogDirs() throws IllegalArgumentException, IOException {
		FileSystem dfs = FileSystem.get(new Configuration());

		logger.info("Deleting lareferenciaLog directory: " + ExecuteWorkflow.lareferenciaLogPath);
		dfs.delete(new Path(ExecuteWorkflow.lareferenciaLogPath), true);

		logger.info("Creating lareferenciaLog directory: " + ExecuteWorkflow.lareferenciaLogPath);
		dfs.mkdirs(new Path(ExecuteWorkflow.lareferenciaLogPath));
	}

	private void createTables() throws Exception {
		try {
			Statement stmt = ConnectDB.getHiveConnection().createStatement();

			logger.info("Creating LaReferencia tables");
			String sqlCreateTableLareferenciaLog = "CREATE TABLE IF NOT EXISTS "
					+ ConnectDB.getUsageStatsDBSchema() + ".lareferencialog(matomoid INT, "
					+ "source STRING, id_visit STRING, country STRING, action STRING, url STRING, entity_id STRING, "
					+ "source_item_type STRING, timestamp STRING, referrer_name STRING, agent STRING) "
					+ "clustered by (source, id_visit, action, timestamp, entity_id) into 100 buckets "
					+ "stored as orc tblproperties('transactional'='true')";
			stmt.executeUpdate(sqlCreateTableLareferenciaLog);
			logger.info("Created LaReferencia tables");

			stmt.close();
			ConnectDB.getHiveConnection().close();
			logger.info("Lareferencia Tables Created");

		} catch (Exception e) {
			logger.error("Failed to create tables: " + e);
			throw new Exception("Failed to create tables: " + e.toString(), e);
		}
	}

	private String getPiwikLogUrl() {
		return piwikUrl + "/";
	}

	private String getJson(String url) throws Exception {
		try {
			URL website = new URL(url);
			URLConnection connection = website.openConnection();

			logger.info("### getJson for: {}", url);
			StringBuilder response;
			try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				response = new StringBuilder();
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
//					response.append("\n");
				}
			}

			return response.toString();
		} catch (Exception e) {
			logger.error("Failed to get URL: " + e);
			throw new Exception("Failed to get URL: " + e.toString(), e);
		}
	}

	public void GetLaReferenciaRepos(String repoLogsPath) throws Exception {
		String baseApiUrl = getPiwikLogUrl() + ApimethodGetAllSites + format + "&token_auth=" + this.tokenAuth;
		String content = "";

		List<Integer> siteIdsToVisit = new ArrayList<Integer>();

		// Getting all the siteIds in a list for logging reasons & limiting the list
		// to the max number of siteIds
		content = getJson(baseApiUrl);
		JSONParser parser = new JSONParser();
		JSONArray jsonArray = (JSONArray) parser.parse(content);
		for (Object aJsonArray : jsonArray) {
			JSONObject jsonObjectRow = (JSONObject) aJsonArray;
			siteIdsToVisit.add(Integer.parseInt(jsonObjectRow.get("idsite").toString()));
		}
		logger.info("Found the following siteIds for download: " + siteIdsToVisit);

		if (ExecuteWorkflow.numberOfPiwikIdsToDownload > 0
				&& ExecuteWorkflow.numberOfPiwikIdsToDownload <= siteIdsToVisit.size()) {
			logger.info("Trimming siteIds list to the size of: " + ExecuteWorkflow.numberOfPiwikIdsToDownload);
			siteIdsToVisit = siteIdsToVisit.subList(0, ExecuteWorkflow.numberOfPiwikIdsToDownload);
		}

		logger.info("Downloading from repos with the followins siteIds: " + siteIdsToVisit);

		for (int siteId : siteIdsToVisit) {
			logger.info("Now working on LaReferencia MatomoId: " + siteId);
			this.getLaReFerenciaLogs(repoLogsPath, siteId);
		}
	}

	public void getLaReFerenciaLogs(String repoLogsPath, int laReferencialMatomoID) throws Exception {
		logger.info("Downloading logs for LaReferencia repoid {}", laReferencialMatomoID);

		// Get the latest timestamp from the logs
		PreparedStatement st = ConnectDB
				.getHiveConnection()
				.prepareStatement(
						"SELECT max(timestamp) FROM " + ConnectDB.getUsageStatsDBSchema()
								+ ".lareferencialog WHERE matomoid=?");
		st.setInt(1, laReferencialMatomoID);
		ResultSet rs_date = st.executeQuery();

		LocalDate dateMax = null;
		while (rs_date.next()) {
			String dateStr = rs_date.getString(1);
			logger.info("### dateStr: {}", dateStr);

			if (dateStr != null && !"null".equals(dateStr) && !dateStr.isEmpty()) {
				dateMax = LocalDateTime.parse(dateStr, YYYY_MM_DD_HH_mm_ss).toLocalDate();
				start = dateMax;
			}
		}
		rs_date.close();

		initializeDateRange(dateMax);
		for (LocalDate currentDay = start; !currentDay.isAfter(end); currentDay = currentDay.plusDays(1)) {
			if (dateMax != null && !currentDay.isAfter(dateMax)) {
				logger
						.info(
								"Date found in logs " + dateMax + " and not downloading Matomo logs for "
										+ laReferencialMatomoID);
				continue;
			}

			logger
					.info(
							"Downloading logs for LaReferencia repoid {} and for {}", laReferencialMatomoID,
							currentDay.format(YYYY_MM_DD_FORMAT));

			String period = "&period=day&date=" + currentDay.format(YYYY_MM_DD_FORMAT);
			String outFolder = repoLogsPath;

			FileSystem fs = FileSystem.get(new Configuration());
			String filename = outFolder + "/" + laReferencialMatomoID + "_LaRefPiwiklog"
					+ currentDay.format(YYYY_MM_DD_FORMAT) + ".json";
			FSDataOutputStream fin = fs.create(new Path(filename), true);

			String baseApiUrl = getPiwikLogUrl() + APImethod + "&idSite=" + laReferencialMatomoID + period + format
					+ "&expanded=5&filter_limit=10&token_auth=" + tokenAuth;

			int i = 0;
			String content;
			JSONParser parser = new JSONParser();

			do {
				String apiUrl = baseApiUrl;
				if (i > 0) {
					apiUrl += "&filter_offset=" + (i * 1000);
				}

				content = getJson(apiUrl);
				if (content.isEmpty() || content.equals("[]") || content.contains("\"result\":\"error\"")) {
					break;
				}

				JSONArray jsonArray = (JSONArray) parser.parse(content);
				for (Object aJsonArray : jsonArray) {
					JSONObject jsonObjectRaw = (JSONObject) aJsonArray;
					fin.write(jsonObjectRaw.toJSONString().getBytes());
					fin.writeChar('\n');
				}

				logger
						.info(
								"Downloaded part " + i + " of logs for LaReferencia repoid " + laReferencialMatomoID
										+ " and for " + currentDay.format(YYYY_MM_DD_FORMAT));
				i++;
			} while (true);

			fin.close();
		}
	}

	private void initializeDateRange(LocalDate maxDate) {
		if (maxDate != null) {
			start = maxDate.plusDays(1); // start from the next day, because maxDate has been already processed
		} else {
			start = ExecuteWorkflow.startingLogPeriod; // no maxDate? then get the start date from config
		}

		// Add number of days
		end = start.plusDays(NUM_OF_DAYS);

		// Ensure end date is not after yesterday
		LocalDate yesterday = LocalDate.now().minusDays(1);
		if (end.isAfter(yesterday)) {
			end = yesterday;
		}

		logger.info("Starting period for log download: {}", YYYY_MM_DD_FORMAT.format(start));
		logger
				.info(
						"Ending period for log download ({} days or yesterday): {}", NUM_OF_DAYS,
						YYYY_MM_DD_FORMAT.format(end));
	}

}
