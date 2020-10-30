
package eu.dnetlib.oa.graph.usagestats.export;

import java.io.*;
import java.net.Authenticator;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author D. Pierrakos, S. Zoupanos
 */
public class PiwikDownloadLogs {

	private final String piwikUrl;
	private Date startDate;
	private final String tokenAuth;

	/*
	 * The Piwik's API method
	 */
	private final String APImethod = "?module=API&method=Live.getLastVisitsDetails";
	private final String format = "&format=json";

	private static final Logger logger = LoggerFactory.getLogger(PiwikDownloadLogs.class);

	public PiwikDownloadLogs(String piwikUrl, String tokenAuth) {
		this.piwikUrl = piwikUrl;
		this.tokenAuth = tokenAuth;

	}

	private String getPiwikLogUrl() {
		return "https://" + piwikUrl + "/";
	}

	private String getJson(String url) throws Exception {
		try {
			logger.debug("Connecting to download the JSON: " + url);
			URL website = new URL(url);
			URLConnection connection = website.openConnection();

			StringBuilder response;
			try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				response = new StringBuilder();
				String inputLine;
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
			}
			return response.toString();
		} catch (Exception e) {
			logger.error("Failed to get URL: " + url + " Exception: " + e);
			throw new Exception("Failed to get URL: " + url + " Exception: " + e.toString(), e);
		}
	}

	class WorkerThread implements Runnable {
		private Calendar currDay;
		private int siteId;
		private String repoLogsPath;
		private String portalLogPath;
		private String portalMatomoID;

		public WorkerThread(Calendar currDay, int siteId, String repoLogsPath, String portalLogPath,
			String portalMatomoID) throws IOException {
			this.currDay = (Calendar) currDay.clone();
			this.siteId = new Integer(siteId);
			this.repoLogsPath = new String(repoLogsPath);
			this.portalLogPath = new String(portalLogPath);
			this.portalMatomoID = new String(portalMatomoID);
		}

		public void run() {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			System.out
				.println(
					Thread.currentThread().getName() + " (Start) Thread for "
						+ "parameters: currDay=" + sdf.format(currDay.getTime()) + ", siteId=" + siteId +
						", repoLogsPath=" + repoLogsPath + ", portalLogPath=" + portalLogPath +
						", portalLogPath=" + portalLogPath + ", portalMatomoID=" + portalMatomoID);
			try {
				GetOpenAIRELogsForDate(currDay, siteId, repoLogsPath, portalLogPath, portalMatomoID);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out
				.println(
					Thread.currentThread().getName() + " (End) Thread for "
						+ "parameters: currDay=" + sdf.format(currDay.getTime()) + ", siteId=" + siteId +
						", repoLogsPath=" + repoLogsPath + ", portalLogPath=" + portalLogPath +
						", portalLogPath=" + portalLogPath + ", portalMatomoID=" + portalMatomoID);
		}

		public void GetOpenAIRELogsForDate(Calendar currDay, int siteId, String repoLogsPath, String portalLogPath,
			String portalMatomoID) throws Exception {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

			Date date = currDay.getTime();
			logger.info("Downloading logs for repoid " + siteId + " and for " + sdf.format(date));

			String period = "&period=day&date=" + sdf.format(date);
			String outFolder = "";
			if (siteId == Integer.parseInt(portalMatomoID)) {
				outFolder = portalLogPath;
			} else {
				outFolder = repoLogsPath;
			}

			String baseApiUrl = getPiwikLogUrl() + APImethod + "&idSite=" + siteId + period + format
				+ "&expanded=5&filter_limit=1000&token_auth=" + tokenAuth;
			String content = "";

			int i = 0;

			JSONParser parser = new JSONParser();
			StringBuffer totalContent = new StringBuffer();
			FileSystem fs = FileSystem.get(new Configuration());

			do {
				int writtenBytes = 0;
				String apiUrl = baseApiUrl;

				if (i > 0) {
					apiUrl += "&filter_offset=" + (i * 1000);
				}

				content = getJson(apiUrl);
				if (content.length() == 0 || content.equals("[]"))
					break;

				FSDataOutputStream fin = fs
					.create(
						new Path(outFolder + "/" + siteId + "_Piwiklog" + sdf.format((date)) + "_offset_" + i
							+ ".json"),
						true);
				JSONArray jsonArray = (JSONArray) parser.parse(content);
				for (Object aJsonArray : jsonArray) {
					JSONObject jsonObjectRaw = (JSONObject) aJsonArray;
					byte[] jsonObjectRawBytes = jsonObjectRaw.toJSONString().getBytes();
					fin.write(jsonObjectRawBytes);
					fin.writeChar('\n');

					writtenBytes += jsonObjectRawBytes.length + 1;
				}

				fin.close();
				System.out
					.println(
						Thread.currentThread().getName() + " (Finished writing) Wrote " + writtenBytes
							+ " bytes. Filename: " + siteId + "_Piwiklog" + sdf.format((date)) + "_offset_" + i
							+ ".json");

				i++;
			} while (true);

			fs.close();
		}
	}

	public void GetOpenAIRELogs(String repoLogsPath, String portalLogPath, String portalMatomoID) throws Exception {

		Statement statement = ConnectDB.getHiveConnection().createStatement();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		ResultSet rs = statement
			.executeQuery(
				"SELECT distinct piwik_id from " + ConnectDB.getStatsDBSchema()
					+ ".datasource where piwik_id is not null and piwik_id <> 0 order by piwik_id");

		// Getting all the piwikids in a list for logging reasons & limitting the list
		// to the max number of piwikids
		List<Integer> piwikIdToVisit = new ArrayList<Integer>();
		while (rs.next())
			piwikIdToVisit.add(rs.getInt(1));
		logger.info("Found the following piwikIds for download: " + piwikIdToVisit);

		if (ExecuteWorkflow.numberOfPiwikIdsToDownload > 0 &&
			ExecuteWorkflow.numberOfPiwikIdsToDownload <= piwikIdToVisit.size()) {
			logger.info("Trimming piwikIds list to the size of: " + ExecuteWorkflow.numberOfPiwikIdsToDownload);
			piwikIdToVisit = piwikIdToVisit.subList(0, ExecuteWorkflow.numberOfPiwikIdsToDownload);
		}

		logger.info("Downloading from repos with the followins piwikIds: " + piwikIdToVisit);

		// Setting the starting period
		Calendar start = (Calendar) ExecuteWorkflow.startingLogPeriod.clone();
		logger.info("Starting period for log download: " + sdf.format(start.getTime()));

		// Setting the ending period (last day of the month)
		Calendar end = (Calendar) ExecuteWorkflow.endingLogPeriod.clone();
		end.add(Calendar.MONTH, +1);
		end.add(Calendar.DAY_OF_MONTH, -1);
		logger.info("Ending period for log download: " + sdf.format(end.getTime()));

		//ExecutorService executor = Executors.newFixedThreadPool(ExecuteWorkflow.numberOfDownloadThreads);
		for (int siteId : piwikIdToVisit) {

			logger.info("Now working on piwikId: " + siteId);

			PreparedStatement st = ConnectDB.DB_HIVE_CONNECTION
				.prepareStatement(
					"SELECT max(timestamp) FROM " + ConnectDB.getUsageStatsDBSchema()
						+ ".piwiklog WHERE source=?");
			st.setInt(1, siteId);

			ResultSet rs_date = st.executeQuery();
			while (rs_date.next()) {
				logger.info("Found max date: " + rs_date.getString(1) + " for repository " + siteId);

				if (rs_date.getString(1) != null && !rs_date.getString(1).equals("null")
					&& !rs_date.getString(1).equals("")) {
					start.setTime(sdf.parse(rs_date.getString(1)));
				}
			}
			rs_date.close();

			for (Calendar currDay = (Calendar) start.clone(); currDay.before(end); currDay.add(Calendar.DATE, 1)) {
				//logger.info("Date used " + currDay.toString());
				//Runnable worker = new WorkerThread(currDay, siteId, repoLogsPath, portalLogPath, portalMatomoID);
				//executor.execute(worker);// calling execute method of ExecutorService
                                GetOpenAIRELogsForDate(currDay, siteId, repoLogsPath, portalLogPath, portalMatomoID);
			}
		}
		//executor.shutdown();
		//while (!executor.isTerminated()) {
		//}
		//System.out.println("Finished all threads");
	}
        
        		public void GetOpenAIRELogsForDate(Calendar currDay, int siteId, String repoLogsPath, String portalLogPath,
			String portalMatomoID) throws Exception {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

			Date date = currDay.getTime();
			logger.info("Downloading logs for repoid " + siteId + " and for " + sdf.format(date));

			String period = "&period=day&date=" + sdf.format(date);
			String outFolder = "";
			if (siteId == Integer.parseInt(portalMatomoID)) {
				outFolder = portalLogPath;
			} else {
				outFolder = repoLogsPath;
			}

			String baseApiUrl = getPiwikLogUrl() + APImethod + "&idSite=" + siteId + period + format
				+ "&expanded=5&filter_limit=1000&token_auth=" + tokenAuth;
			String content = "";

			int i = 0;

			JSONParser parser = new JSONParser();
			StringBuffer totalContent = new StringBuffer();
			FileSystem fs = FileSystem.get(new Configuration());

			do {
				int writtenBytes = 0;
				String apiUrl = baseApiUrl;

				if (i > 0) {
					apiUrl += "&filter_offset=" + (i * 1000);
				}

				content = getJson(apiUrl);
				if (content.length() == 0 || content.equals("[]"))
					break;

				FSDataOutputStream fin = fs
					.create(
						new Path(outFolder + "/" + siteId + "_Piwiklog" + sdf.format((date)) + "_offset_" + i
							+ ".json"),
						true);
				JSONArray jsonArray = (JSONArray) parser.parse(content);
				for (Object aJsonArray : jsonArray) {
					JSONObject jsonObjectRaw = (JSONObject) aJsonArray;
					byte[] jsonObjectRawBytes = jsonObjectRaw.toJSONString().getBytes();
					fin.write(jsonObjectRawBytes);
					fin.writeChar('\n');

					writtenBytes += jsonObjectRawBytes.length + 1;
				}

				fin.close();
				System.out
					.println(
						Thread.currentThread().getName() + " (Finished writing) Wrote " + writtenBytes
							+ " bytes. Filename: " + siteId + "_Piwiklog" + sdf.format((date)) + "_offset_" + i
							+ ".json");

				i++;
			} while (true);

			fs.close();
		}
}
