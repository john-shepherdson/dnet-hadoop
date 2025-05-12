
package eu.dnetlib.oa.graph.usagerawdata.export;

import static eu.dnetlib.oa.graph.usagerawdata.export.ExecuteWorkflow.numberOfDaysToProcess;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PiwikDownloadLogs {

	private final String piwikUrl;
	private final String tokenAuth;

	private final String APImethod = "?module=API&method=Live.getLastVisitsDetails";
	private final String format = "&format=json";

	private static final Logger logger = LoggerFactory.getLogger(PiwikDownloadLogs.class);

	Map<Integer, List<LocalDateTime>> siteIdsWithNoDataDates = new HashMap<>();

	private static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS_FORMAT = DateTimeFormatter
		.ofPattern("yyyy-MM-dd HH:mm:ss");
	private static final DateTimeFormatter YYYY_MM_DD_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	// Get start/end period
	private LocalDate start;
	private LocalDate end;

	public PiwikDownloadLogs(String piwikUrl, String tokenAuth) {
		this.piwikUrl = piwikUrl;
		this.tokenAuth = tokenAuth;
	}

	private String getPiwikLogUrl() {
		return "https://" + piwikUrl + "/";
	}

	public void getOpenAIRELogs(String repoLogsPath, String portalLogPath, String portalMatomoID) throws Exception {
		logger.info("### numberOfDaysToProcess: {}", numberOfDaysToProcess);

		Statement statement = ConnectDB.getHiveConnection().createStatement();
		ResultSet rs = statement
			.executeQuery(
				"SELECT distinct piwik_id from " + ConnectDB.getStatsDBSchema()
					+ ".datasource where piwik_id is not null and piwik_id <> 0 order by piwik_id");

		List<Integer> piwikIdToVisit = new ArrayList<>();
		while (rs.next()) {
			piwikIdToVisit.add(rs.getInt(1));
		}
		logger.info("Found the following piwikIds for download: {}", piwikIdToVisit);

		if (ExecuteWorkflow.numberOfPiwikIdsToDownload > 0
			&& ExecuteWorkflow.numberOfPiwikIdsToDownload <= piwikIdToVisit.size()) {
			logger.info("Trimming piwikIds list to the size of: " + ExecuteWorkflow.numberOfPiwikIdsToDownload);
			piwikIdToVisit = piwikIdToVisit.subList(0, ExecuteWorkflow.numberOfPiwikIdsToDownload);
		}

		logger.info("Downloading from repos with the following piwikIds: {}", piwikIdToVisit);

		Map<Integer, LocalDateTime> maxTimestamps = getMaxTimestamps();

		for (int siteId : piwikIdToVisit) {
			logger.info("### (1st loop) - working on piwikId: {}", siteId);

			LocalDate dateMax = null;
			LocalDateTime maxTimestamp = maxTimestamps.get(siteId);

			if (maxTimestamp != null) {
				dateMax = maxTimestamp.toLocalDate();
				logger.info("### Found max date: {} for siteId {}", maxTimestamp, siteId);
			}

			initializeDateRange(dateMax);
			for (LocalDate currDay = start; currDay.isBefore(end); currDay = currDay.plusDays(1)) {
				logger
					.info(
						"### (2nd loop) - Downloading Matomo logs - siteId: {}, date: {}", siteId,
						currDay.format(YYYY_MM_DD_DATE_FORMAT));
				getOpenAIRELogsForDate(currDay, siteId, repoLogsPath, portalLogPath, portalMatomoID);
			} // end 2nd loop
		} // end 1st loop

		findNoDataMaxDateAndInsertPiwik();
	}

	private void initializeDateRange(LocalDate maxDate) {
		if (maxDate != null) {
			start = maxDate.plusDays(1); // start from the next day, because maxDate has been already processed
		} else {
			start = ExecuteWorkflow.startingLogPeriod; // no maxDate? then get the start date from config
		}

		// Add number of days
		end = start.plusDays(numberOfDaysToProcess); // number of days to process since the start date

		// Ensure end date is not after this date (give time to Matomo to process data).
		LocalDate twoDaysAgo = LocalDate.now().minusDays(2);
		if (end.isAfter(twoDaysAgo)) {
			end = twoDaysAgo;
		}

		logger.info("Starting period for log download: {}", YYYY_MM_DD_DATE_FORMAT.format(start));
		logger
			.info(
				"Ending period for log download ({} days or yesterday): {}", numberOfDaysToProcess,
				YYYY_MM_DD_DATE_FORMAT.format(end));
	}

	private void getOpenAIRELogsForDate(LocalDate currDay, int siteId, String repoLogsPath, String portalLogPath,
		String portalMatomoID) throws Exception {
		logger.info("### Downloading logs - siteId: {}, date: {}", siteId, currDay.format(YYYY_MM_DD_DATE_FORMAT));

		String period = "&period=day&date=" + currDay.format(YYYY_MM_DD_DATE_FORMAT);
		String outFolder = siteId == Integer.parseInt(portalMatomoID) ? portalLogPath : repoLogsPath;

		if (siteId == Integer.parseInt(portalMatomoID)) {
			logger.info("### portalMatomoID");
		} else {
			logger.info("### not portalMatomoID");
		}

		String baseApiUrl = getPiwikLogUrl() + APImethod + "&idSite=" + siteId + period + format
			+ "&expanded=5&filter_limit=1000&token_auth=" + tokenAuth;
		String content;

		int i = 0;
		JSONParser parser = new JSONParser();
		FileSystem fs = FileSystem.get(new Configuration());

		do {
			int writtenBytes = 0;
			String apiUrl = baseApiUrl;

			if (i > 0) {
				apiUrl += "&filter_offset=" + (i * 1000);
			}

			content = getDataFromMatomo(apiUrl);

			if (content.isEmpty() || content.equals("[]") || content.contains("\"result\":\"error\"")) {
				logger.info("### content error: {}", content);

				if (i == 0) {
					LocalDateTime missingDateTime = currDay.atStartOfDay();
					logger
						.info(
							"### no content for siteId: {} for date: {} (store in siteIdsWithNoDataDates)", siteId,
							missingDateTime.format(YYYY_MM_DD_DATE_FORMAT));
					siteIdsWithNoDataDates.computeIfAbsent(siteId, k -> new ArrayList<>()).add(missingDateTime);
				}

				break;
			}

			JSONArray jsonArray = (JSONArray) parser.parse(content);

			String pathString = outFolder + "/" + siteId + "_Piwiklog" + currDay.format(YYYY_MM_DD_DATE_FORMAT)
				+ "_offset_" + i + ".json";
			FSDataOutputStream fin = fs.create(new Path(pathString), true);
			logger.info("Writing data - Path with filename: {}", pathString);

			for (Object aJsonArray : jsonArray) {
				JSONObject jsonObjectRaw = (JSONObject) aJsonArray;
				byte[] jsonObjectRawBytes = jsonObjectRaw.toJSONString().getBytes();
				fin.write(jsonObjectRawBytes);
				fin.writeChar('\n');

				writtenBytes += jsonObjectRawBytes.length + 1;
			}

			fin.close();
			logger.info("(Finished writing) - Wrote: {} bytes. Filename: {}", writtenBytes, pathString);

			i++;
		} while (true);

		fs.close();
	}

	private String getDataFromMatomo(String url) throws Exception {
		try {
			logger.info("Connecting to download the JSON: " + url);
			URL website = new URL(url);
			URLConnection connection = website.openConnection();

			StringBuilder response;
			try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				response = new StringBuilder();
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					if (inputLine.contains("\"result\":\"error\"")) {
						logger.error("matomo response is invalid: {}", inputLine);
						break;
					}

					response.append(inputLine);
				}
			}
			return response.toString();
		} catch (Exception e) {
			logger.error("Piwik - Failed to get URL: {} Exception: {}", url, e.toString());
			throw new Exception("Failed to get URL: " + url + " Exception: " + e, e);
		}
	}

	private Map<Integer, LocalDateTime> getMaxTimestamps() throws SQLException {
		logger.info("### get MAX Timestamps for all siteIDs ###");
		Map<Integer, LocalDateTime> resultMap = new HashMap<>();

		PreparedStatement st = ConnectDB.DB_HIVE_CONNECTION
			.prepareStatement(
				"SELECT source, MAX(`timestamp`) from " + ConnectDB.getUsageStatsDBSchema()
					+ ".piwiklog GROUP BY source");
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Integer siteId = rs.getInt(1);
			String timestampStr = rs.getString(2);
			logger.info("### siteID: {}, timestamp: {}", siteId, timestampStr);

			if (timestampStr != null) {
				LocalDateTime timestamp = LocalDateTime.parse(timestampStr, YYYY_MM_DD_HH_MM_SS_FORMAT);
				resultMap.put(siteId, timestamp); // e.g.: [110, 2019-07-30 18:22:51]
			}
		}

		resultMap
			.forEach(
				(siteId, timestamp) -> logger
					.info("[getMaxTimestamps] - siteId: {}, Max Timestamp: {}", siteId, timestamp));
		return resultMap;
	}

	private void findNoDataMaxDateAndInsertPiwik() {
		logger.info("### siteIdsWithNoDataDates size: {}", siteIdsWithNoDataDates.size());
		Map<Integer, LocalDateTime> maxDatesWithNoData = new HashMap<>();

		for (Map.Entry<Integer, List<LocalDateTime>> entry : siteIdsWithNoDataDates.entrySet()) {
			int siteId = entry.getKey();
			List<LocalDateTime> dates = entry.getValue();

			if (dates.isEmpty())
				continue;

			LocalDateTime maxDate = Collections.max(dates);
			maxDatesWithNoData.put(siteId, maxDate);
		}

		for (Map.Entry<Integer, LocalDateTime> entry : maxDatesWithNoData.entrySet()) {
			logger
				.info(
					"### findMaxDateForEachSite - SiteId: {} -> Max Missing Date: {}", entry.getKey(),
					entry.getValue().format(YYYY_MM_DD_HH_MM_SS_FORMAT));
		}

		dummyInsertPiwiklog(maxDatesWithNoData);
	}

	private void dummyInsertPiwiklog(Map<Integer, LocalDateTime> maxDatesWithNoData) {
		logger.info("### insert Piwiklog MaxTimestamp");

		String insertQuery = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklog (source, timestamp, action) VALUES (?, ?, ?)";

		try (PreparedStatement st = ConnectDB.getHiveConnection().prepareStatement(insertQuery)) {
			for (Map.Entry<Integer, LocalDateTime> entry : maxDatesWithNoData.entrySet()) {
				int siteId = entry.getKey();
				LocalDateTime newTimestamp = entry.getValue();

				String newTimestampStr = newTimestamp.format(YYYY_MM_DD_HH_MM_SS_FORMAT);

				st.setInt(1, siteId);
				st.setString(2, newTimestampStr);
				st.setString(3, "dummy_action");

				st.executeUpdate();

				logger.info("Inserted new max timestamp for siteId: {}. New timestamp: {}", siteId, newTimestampStr);
			}
		} catch (SQLException e) {
			logger.error("Failed to insert max timestamp", e);
		}
	}
}
