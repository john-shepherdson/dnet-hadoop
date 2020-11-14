package eu.dnetlib.oa.graph.usagerawdata.export;

import java.io.*;
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
public class LaReferenciaDownloadLogs {

    private final String piwikUrl;
    private Date startDate;
    private final String tokenAuth;

    /*
	 * The Piwik's API method
     */
    private final String APImethod = "?module=API&method=Live.getLastVisitsDetails";
    private final String format = "&format=json";
    private final String ApimethodGetAllSites = "?module=API&method=SitesManager.getSitesWithViewAccess";

    private static final Logger logger = LoggerFactory.getLogger(LaReferenciaDownloadLogs.class);

    public LaReferenciaDownloadLogs(String piwikUrl, String tokenAuth) throws Exception {
        this.piwikUrl = piwikUrl;
        this.tokenAuth = tokenAuth;
        this.createTables();
//        this.createTmpTables();
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
//            String sqlcreateRuleLaReferenciaLog = "CREATE OR REPLACE RULE ignore_duplicate_inserts AS "
//                    + " ON INSERT TO lareferencialog "
//                    + " WHERE (EXISTS ( SELECT lareferencialog.matomoid, lareferencialog.source, lareferencialog.id_visit,"
//                    + "lareferencialog.action, lareferencialog.\"timestamp\", lareferencialog.entity_id "
//                    + "FROM lareferencialog "
//                    + "WHERE lareferencialog.matomoid=new.matomoid AND lareferencialog.source = new.source AND lareferencialog.id_visit = new.id_visit AND lareferencialog.action = new.action AND lareferencialog.entity_id = new.entity_id AND lareferencialog.\"timestamp\" = new.\"timestamp\")) DO INSTEAD NOTHING;";
//            String sqlCreateRuleIndexLaReferenciaLog = "create index if not exists lareferencialog_rule on lareferencialog(matomoid, source, id_visit, action, entity_id, \"timestamp\");";
//            stmt.executeUpdate(sqlcreateRuleLaReferenciaLog);
//            stmt.executeUpdate(sqlCreateRuleIndexLaReferenciaLog);

            stmt.close();
            ConnectDB.getHiveConnection().close();
            logger.info("Lareferencia Tables Created");

        } catch (Exception e) {
            logger.error("Failed to create tables: " + e);
            throw new Exception("Failed to create tables: " + e.toString(), e);
            // System.exit(0);
        }
    }

//	private void createTmpTables() throws Exception {
//
//		try {
//			Statement stmt = ConnectDB.getConnection().createStatement();
//			String sqlCreateTmpTableLaReferenciaLog = "CREATE TABLE IF NOT EXISTS lareferencialogtmp(matomoid INTEGER, source TEXT, id_visit TEXT, country TEXT, action TEXT, url TEXT, entity_id TEXT, source_item_type TEXT, timestamp TEXT, referrer_name TEXT, agent TEXT, PRIMARY KEY(source, id_visit, action, timestamp, entity_id));";
//			String sqlcreateTmpRuleLaReferenciaLog = "CREATE OR REPLACE RULE ignore_duplicate_inserts AS "
//				+ " ON INSERT TO lareferencialogtmp "
//				+ " WHERE (EXISTS ( SELECT lareferencialogtmp.matomoid, lareferencialogtmp.source, lareferencialogtmp.id_visit,"
//				+ "lareferencialogtmp.action, lareferencialogtmp.\"timestamp\", lareferencialogtmp.entity_id "
//				+ "FROM lareferencialogtmp "
//				+ "WHERE lareferencialogtmp.matomoid=new.matomoid AND lareferencialogtmp.source = new.source AND lareferencialogtmp.id_visit = new.id_visit AND lareferencialogtmp.action = new.action AND lareferencialogtmp.entity_id = new.entity_id AND lareferencialogtmp.\"timestamp\" = new.\"timestamp\")) DO INSTEAD NOTHING;";
//			stmt.executeUpdate(sqlCreateTmpTableLaReferenciaLog);
//			stmt.executeUpdate(sqlcreateTmpRuleLaReferenciaLog);
//
//			stmt.close();
//			log.info("Lareferencia Tmp Tables Created");
//
//		} catch (Exception e) {
//			log.error("Failed to create tmptables: " + e);
//			throw new Exception("Failed to create tmp tables: " + e.toString(), e);
//			// System.exit(0);
//		}
//	}
    private String getPiwikLogUrl() {
        return piwikUrl + "/";
    }

    private String getJson(String url) throws Exception {
        try {
            URL website = new URL(url);
            URLConnection connection = website.openConnection();

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
            this.GetLaReFerenciaLogs(repoLogsPath, siteId);
        }
    }

    public void GetLaReFerenciaLogs(String repoLogsPath,
            int laReferencialMatomoID) throws Exception {

        logger.info("Downloading logs for LaReferencia repoid " + laReferencialMatomoID);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        // Setting the starting period
        Calendar start = (Calendar) ExecuteWorkflow.startingLogPeriod.clone();
        logger.info("Starting period for log download: " + sdf.format(start.getTime()));

        // Setting the ending period (last day of the month)
        Calendar end = (Calendar) ExecuteWorkflow.endingLogPeriod.clone();
        end.add(Calendar.MONTH, +1);
        end.add(Calendar.DAY_OF_MONTH, -1);
        logger.info("Ending period for log download: " + sdf.format(end.getTime()));

        PreparedStatement st = ConnectDB
                .getHiveConnection()
                .prepareStatement(
                        "SELECT max(timestamp) FROM " + ConnectDB.getUsageStatsDBSchema()
                        + ".lareferencialog WHERE matomoid=?");
        st.setInt(1, laReferencialMatomoID);
        Date dateMax = null;

        ResultSet rs_date = st.executeQuery();
        while (rs_date.next()) {
            if (rs_date.getString(1) != null && !rs_date.getString(1).equals("null")
                    && !rs_date.getString(1).equals("")) {
                start.setTime(sdf.parse(rs_date.getString(1)));
                dateMax = sdf.parse(rs_date.getString(1));
            }
        }
        rs_date.close();

        for (Calendar currDay = (Calendar) start.clone(); currDay.before(end); currDay.add(Calendar.DATE, 1)) {
            Date date = currDay.getTime();
            if (dateMax != null && currDay.getTime().compareTo(dateMax) <= 0) {
                logger.info("Date found in logs " + dateMax + " and not downloanding Matomo logs for " + laReferencialMatomoID);
            } else {
                logger
                        .info(
                                "Downloading logs for LaReferencia repoid " + laReferencialMatomoID + " and for "
                                + sdf.format(date));

                String period = "&period=day&date=" + sdf.format(date);
                String outFolder = "";
                outFolder = repoLogsPath;

                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream fin = fs
                        .create(
                                new Path(outFolder + "/" + laReferencialMatomoID + "_LaRefPiwiklog" + sdf.format((date)) + ".json"),
                                true);

                String baseApiUrl = getPiwikLogUrl() + APImethod + "&idSite=" + laReferencialMatomoID + period + format
                        + "&expanded=5&filter_limit=1000&token_auth=" + tokenAuth;
                String content = "";
                int i = 0;

                JSONParser parser = new JSONParser();
                do {
                    String apiUrl = baseApiUrl;

                    if (i > 0) {
                        apiUrl += "&filter_offset=" + (i * 1000);
                    }

                    content = getJson(apiUrl);
                    if (content.length() == 0 || content.equals("[]")) {
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
                                    + " and for "
                                    + sdf.format(date));
                    i++;
                } while (true);
                fin.close();
            }
        }
    }
}
