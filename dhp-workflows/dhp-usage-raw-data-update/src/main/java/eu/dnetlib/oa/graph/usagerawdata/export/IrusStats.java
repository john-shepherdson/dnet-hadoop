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
public class IrusStats {

    private String irusUKURL;

    private static final Logger logger = LoggerFactory.getLogger(IrusStats.class);

    public IrusStats(String irusUKURL) throws Exception {
        this.irusUKURL = irusUKURL;
        // The following may not be needed - It will be created when JSON tables are created
//		createTmpTables();
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

            // To see how to apply to the ignore duplicate rules and indexes
//			stmt.executeUpdate(sqlCreateTableSushiLog);
//			String sqlcreateRuleSushiLog = "CREATE OR REPLACE RULE ignore_duplicate_inserts AS "
//				+ " ON INSERT TO sushilog "
//				+ " WHERE (EXISTS ( SELECT sushilog.source, sushilog.repository,"
//				+ "sushilog.rid, sushilog.date "
//				+ "FROM sushilog "
//				+ "WHERE sushilog.source = new.source AND sushilog.repository = new.repository AND sushilog.rid = new.rid AND sushilog.date = new.date AND sushilog.metric_type = new.metric_type)) DO INSTEAD NOTHING;";
//			stmt.executeUpdate(sqlcreateRuleSushiLog);
//			String createSushiIndex = "create index if not exists sushilog_duplicates on sushilog(source, repository, rid, date, metric_type);";
//			stmt.executeUpdate(createSushiIndex);
            stmt.close();
            ConnectDB.getHiveConnection().close();
            logger.info("Sushi Tables Created");
        } catch (Exception e) {
            logger.error("Failed to create tables: " + e);
            throw new Exception("Failed to create tables: " + e.toString(), e);
        }
    }

//	// The following may not be needed - It will be created when JSON tables are created
//	private void createTmpTables() throws Exception {
//		try {
//
//			Statement stmt = ConnectDB.getConnection().createStatement();
//			String sqlCreateTableSushiLog = "CREATE TABLE IF NOT EXISTS sushilogtmp(source TEXT, repository TEXT, rid TEXT, date TEXT, metric_type TEXT, count INT, PRIMARY KEY(source, repository, rid, date, metric_type));";
//			stmt.executeUpdate(sqlCreateTableSushiLog);
//
//			// stmt.executeUpdate("CREATE TABLE IF NOT EXISTS public.sushilog AS TABLE sushilog;");
//			// String sqlCopyPublicSushiLog = "INSERT INTO sushilog SELECT * FROM public.sushilog;";
//			// stmt.executeUpdate(sqlCopyPublicSushiLog);
//			String sqlcreateRuleSushiLog = "CREATE OR REPLACE RULE ignore_duplicate_inserts AS "
//				+ " ON INSERT TO sushilogtmp "
//				+ " WHERE (EXISTS ( SELECT sushilogtmp.source, sushilogtmp.repository,"
//				+ "sushilogtmp.rid, sushilogtmp.date "
//				+ "FROM sushilogtmp "
//				+ "WHERE sushilogtmp.source = new.source AND sushilogtmp.repository = new.repository AND sushilogtmp.rid = new.rid AND sushilogtmp.date = new.date AND sushilogtmp.metric_type = new.metric_type)) DO INSTEAD NOTHING;";
//			stmt.executeUpdate(sqlcreateRuleSushiLog);
//
//			stmt.close();
//			ConnectDB.getConnection().close();
//			log.info("Sushi Tmp Tables Created");
//		} catch (Exception e) {
//			log.error("Failed to create tables: " + e);
//			throw new Exception("Failed to create tables: " + e.toString(), e);
//		}
//	}
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
        
        logger.info("Creating downloads_stats table");
        String createDownloadsStats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".downloads_stats "
                + "(`source` string, "
                + "`repository_id` string, "
                + "`result_id` string, "
                + "`date`	string, "
                + "`count` bigint,	"
                + "`openaire`	bigint)";
        stmt.executeUpdate(createDownloadsStats);
        logger.info("Created downloads_stats table");

        logger.info("Inserting into downloads_stats");
        String insertDStats = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats "
                + "SELECT s.source, d.id AS repository_id, "
                + "ro.id as result_id, CONCAT(YEAR(date), '/', LPAD(MONTH(date), 2, '0')) as date, s.count, '0' "
                + "FROM " + ConnectDB.getUsageStatsDBSchema() + ".irus_sushilogtmp s, "
                + ConnectDB.getStatsDBSchema() + ".datasource_oids d, "
                + ConnectDB.getStatsDBSchema() + ".result_oids ro "
                + "WHERE s.repository=d.oid AND s.rid=ro.oid AND metric_type='ft_total' AND s.source='IRUS-UK'";
        stmt.executeUpdate(insertDStats);
        logger.info("Inserted into downloads_stats");

        logger.info("Creating sushilog table");
        String createSushilog = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".sushilog "
                + "(`source` string, "
                + "`repository_id` string, "
                + "`rid` string, "
                + "`date`	string, "
                + "`metric_type` string,	"
                + "`count`	int)";
        stmt.executeUpdate(createSushilog);
        logger.info("Created sushilog table");

        logger.info("Inserting to sushilog table");
        String insertToShushilog = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".sushilog SELECT * FROM "
                + ConnectDB.getUsageStatsDBSchema()
                + ".irus_sushilogtmp";
        stmt.executeUpdate(insertToShushilog);
        logger.info("Inserted to sushilog table");

        ConnectDB.getHiveConnection().close();
    }

    public void getIrusRRReport(String irusUKReportPath) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM");
        // Setting the starting period
        Calendar start = (Calendar) ExecuteWorkflow.startingLogPeriod.clone();
        logger.info("(getIrusRRReport) Starting period for log download: " + sdf.format(start.getTime()));

        // Setting the ending period (last day of the month)
        Calendar end = (Calendar) ExecuteWorkflow.endingLogPeriod.clone();
        end.add(Calendar.MONTH, +1);
        end.add(Calendar.DAY_OF_MONTH, -1);
        logger.info("(getIrusRRReport) Ending period for log download: " + sdf.format(end.getTime()));

        String reportUrl = irusUKURL + "GetReport/?Report=RR1&Release=4&RequestorID=OpenAIRE&BeginDate="
                + sdf.format(start.getTime()) + "&EndDate=" + sdf.format(end.getTime())
                + "&RepositoryIdentifier=&ItemDataType=&NewJiscBand=&Granularity=Monthly&Callback=";

        logger.info("(getIrusRRReport) Getting report: " + reportUrl);

        String text = getJson(reportUrl, "", "");

        List<String> opendoarsToVisit = new ArrayList<String>();
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(text);
        jsonObject = (JSONObject) jsonObject.get("ReportResponse");
        jsonObject = (JSONObject) jsonObject.get("Report");
        jsonObject = (JSONObject) jsonObject.get("Report");
        jsonObject = (JSONObject) jsonObject.get("Customer");
        JSONArray jsonArray = (JSONArray) jsonObject.get("ReportItems");
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

        logger.info("(getIrusRRReport) Found the following opendoars for download: " + opendoarsToVisit);

        if (ExecuteWorkflow.irusNumberOfOpendoarsToDownload > 0
                && ExecuteWorkflow.irusNumberOfOpendoarsToDownload <= opendoarsToVisit.size()) {
            logger.info("Trimming siteIds list to the size of: " + ExecuteWorkflow.irusNumberOfOpendoarsToDownload);
            opendoarsToVisit = opendoarsToVisit.subList(0, ExecuteWorkflow.irusNumberOfOpendoarsToDownload);
        }

        logger.info("(getIrusRRReport) Downloading the followins opendoars: " + opendoarsToVisit);

        for (String opendoar : opendoarsToVisit) {
            logger.info("Now working on openDoar: " + opendoar);
            this.getIrusIRReport(opendoar, irusUKReportPath);
        }

        logger.info("(getIrusRRReport) Finished with report: " + reportUrl);
    }

    private void getIrusIRReport(String opendoar, String irusUKReportPath) throws Exception {

        logger.info("(getIrusIRReport) Getting report(s) with opendoar: " + opendoar);

        ConnectDB.getHiveConnection().setAutoCommit(false);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM");

        // Setting the starting period
        Calendar start = (Calendar) ExecuteWorkflow.startingLogPeriod.clone();
        logger.info("(getIrusIRReport) Starting period for log download: " + simpleDateFormat.format(start.getTime()));

        // Setting the ending period (last day of the month)
        Calendar end = (Calendar) ExecuteWorkflow.endingLogPeriod.clone();
        end.add(Calendar.MONTH, +1);
        end.add(Calendar.DAY_OF_MONTH, -1);
        logger.info("(getIrusIRReport) Ending period for log download: " + simpleDateFormat.format(end.getTime()));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        PreparedStatement st = ConnectDB
                .getHiveConnection()
                .prepareStatement(
                        "SELECT max(date) FROM " + ConnectDB.getUsageStatsDBSchema() + ".sushilog WHERE repository=?");
        st.setString(1, "opendoar____::" + opendoar);
        ResultSet rs_date = st.executeQuery();
        Date dateMax = null;
        while (rs_date.next()) {
            if (rs_date.getString(1) != null && !rs_date.getString(1).equals("null")
                    && !rs_date.getString(1).equals("")) {
                start.setTime(sdf.parse(rs_date.getString(1)));
                dateMax = sdf.parse(rs_date.getString(1));
            }
        }
        rs_date.close();
        int batch_size = 0;

        if (dateMax != null && start.getTime().compareTo(dateMax) <= 0) {
            logger.info("Date found in logs " + dateMax + " and not downloanding logs for " + opendoar);
        } else {
            while (start.before(end)) {
                logger.info("date: " + simpleDateFormat.format(start.getTime()));
                String reportUrl = this.irusUKURL + "GetReport/?Report=IR1&Release=4&RequestorID=OpenAIRE&BeginDate="
                        + simpleDateFormat.format(start.getTime()) + "&EndDate=" + simpleDateFormat.format(start.getTime())
                        + "&RepositoryIdentifier=opendoar%3A" + opendoar
                        + "&ItemIdentifier=&ItemDataType=&hasDOI=&Granularity=Monthly&Callback=";
                start.add(Calendar.MONTH, 1);

                logger.info("Downloading file: " + reportUrl);
                String text = getJson(reportUrl, "", "");
                if (text == null) {
                    continue;
                }

                FileSystem fs = FileSystem.get(new Configuration());
                String filePath = irusUKReportPath + "/" + "IrusIRReport_"
                        + opendoar + "_" + simpleDateFormat.format(start.getTime()) + ".json";
                logger.info("Storing to file: " + filePath);
                FSDataOutputStream fin = fs.create(new Path(filePath), true);

                JSONParser parser = new JSONParser();
                JSONObject jsonObject = (JSONObject) parser.parse(text);
                jsonObject = (JSONObject) jsonObject.get("ReportResponse");
                jsonObject = (JSONObject) jsonObject.get("Report");
                jsonObject = (JSONObject) jsonObject.get("Report");
                jsonObject = (JSONObject) jsonObject.get("Customer");
                JSONArray jsonArray = (JSONArray) jsonObject.get("ReportItems");
                if (jsonArray == null) {
                    continue;
                }
                String oai = "";
                for (Object aJsonArray : jsonArray) {
                    JSONObject jsonObjectRow = (JSONObject) aJsonArray;
                    fin.write(jsonObjectRow.toJSONString().getBytes());
                    fin.writeChar('\n');
                }

                fin.close();
            }

        }
        //ConnectDB.getHiveConnection().close();

        logger.info("(getIrusIRReport) Finished downloading report(s) with opendoar: " + opendoar);
    }

    private String getJson(String url) throws Exception {
        try {
            System.out.println("===> Connecting to: " + url);
            URL website = new URL(url);
            System.out.println("Connection url -----> " + url);
            URLConnection connection = website.openConnection();

            // connection.setRequestProperty ("Authorization", "Basic "+encoded);
            StringBuilder response;
            try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                response = new StringBuilder();
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
//					response.append("\n");
                }
            }

            System.out.println("response ====> " + response.toString());

            return response.toString();
        } catch (Exception e) {
            logger.error("Failed to get URL: " + e);
            System.out.println("Failed to get URL: " + e);
            throw new Exception("Failed to get URL: " + e.toString(), e);
        }
    }

    private String getJson(String url, String username, String password) throws Exception {
        // String cred=username+":"+password;
        // String encoded = new sun.misc.BASE64Encoder().encode (cred.getBytes());
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
            logger.error("Failed to get URL", e);
            return null;
        }
    }
}
