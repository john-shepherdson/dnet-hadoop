package eu.dnetlib.oa.graph.usagerawdata.export;

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

    public void reCreateLogDirs() throws IOException {
        FileSystem dfs = FileSystem.get(new Configuration());

        logger.info("Deleting sarcsReport (Array) directory: " + ExecuteWorkflow.sarcsReportPathArray);
        dfs.delete(new Path(ExecuteWorkflow.sarcsReportPathArray), true);

        logger.info("Deleting sarcsReport (NonArray) directory: " + ExecuteWorkflow.sarcsReportPathNonArray);
        dfs.delete(new Path(ExecuteWorkflow.sarcsReportPathNonArray), true);

        logger.info("Creating sarcsReport (Array) directory: " + ExecuteWorkflow.sarcsReportPathArray);
        dfs.mkdirs(new Path(ExecuteWorkflow.sarcsReportPathArray));

        logger.info("Creating sarcsReport (NonArray) directory: " + ExecuteWorkflow.sarcsReportPathNonArray);
        dfs.mkdirs(new Path(ExecuteWorkflow.sarcsReportPathNonArray));
    }

    public void processSarc(String sarcsReportPathArray, String sarcsReportPathNonArray) throws Exception {
        Statement stmt = ConnectDB.getHiveConnection().createStatement();
        ConnectDB.getHiveConnection().setAutoCommit(false);

        logger.info("Adding JSON Serde jar");
        stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
        logger.info("Added JSON Serde jar");

        logger.info("Dropping sarc_sushilogtmp_json_array table");
        String drop_sarc_sushilogtmp_json_array = "DROP TABLE IF EXISTS "
                + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_json_array";
        stmt.executeUpdate(drop_sarc_sushilogtmp_json_array);
        logger.info("Dropped sarc_sushilogtmp_json_array table");

        logger.info("Creating sarc_sushilogtmp_json_array table");
        String create_sarc_sushilogtmp_json_array = "CREATE EXTERNAL TABLE IF NOT EXISTS "
                + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_json_array(\n"
                + "	`ItemIdentifier` ARRAY<\n"
                + "						struct<\n"
                + "							`Type`: STRING,\n"
                + "							`Value`: STRING\n"
                + "							>\n"
                + "						>,\n"
                + "	`ItemPerformance` struct<\n"
                + "						`Period`:  struct<\n"
                + "									`Begin`: STRING,\n"
                + "									`End`: STRING\n"
                + "									>,\n"
                + "						`Instance`:  struct<\n"
                + "									`Count`: STRING,\n"
                + "									`MetricType`: STRING\n"
                + "									>\n"
                + "						>\n"
                + ")"
                + "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n"
                + "LOCATION '" + sarcsReportPathArray + "/'\n"
                + "TBLPROPERTIES (\"transactional\"=\"false\")";
        stmt.executeUpdate(create_sarc_sushilogtmp_json_array);
        logger.info("Created sarc_sushilogtmp_json_array table");

        logger.info("Dropping sarc_sushilogtmp_json_non_array table");
        String drop_sarc_sushilogtmp_json_non_array = "DROP TABLE IF EXISTS "
                + ConnectDB.getUsageStatsDBSchema()
                + ".sarc_sushilogtmp_json_non_array";
        stmt.executeUpdate(drop_sarc_sushilogtmp_json_non_array);
        logger.info("Dropped sarc_sushilogtmp_json_non_array table");

        logger.info("Creating sarc_sushilogtmp_json_non_array table");
        String create_sarc_sushilogtmp_json_non_array = "CREATE EXTERNAL TABLE IF NOT EXISTS "
                + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_json_non_array (\n"
                + "	`ItemIdentifier` struct<\n"
                + "						`Type`: STRING,\n"
                + "						`Value`: STRING\n"
                + "						>,\n"
                + "	`ItemPerformance` struct<\n"
                + "						`Period`:  struct<\n"
                + "									`Begin`: STRING,\n"
                + "									`End`: STRING\n"
                + "									>,\n"
                + "						`Instance`:  struct<\n"
                + "									`Count`: STRING,\n"
                + "									`MetricType`: STRING\n"
                + "									>\n"
                + "						>"
                + ")"
                + "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n"
                + "LOCATION '" + sarcsReportPathNonArray + "/'\n"
                + "TBLPROPERTIES (\"transactional\"=\"false\")";
        stmt.executeUpdate(create_sarc_sushilogtmp_json_non_array);
        logger.info("Created sarc_sushilogtmp_json_non_array table");

        logger.info("Creating sarc_sushilogtmp table");
        String create_sarc_sushilogtmp = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".sarc_sushilogtmp(source STRING, repository STRING, "
                + "rid STRING, date STRING, metric_type STRING, count INT) clustered by (source) into 100 buckets stored as orc "
                + "tblproperties('transactional'='true')";
        stmt.executeUpdate(create_sarc_sushilogtmp);
        logger.info("Created sarc_sushilogtmp table");

        logger.info("Inserting to sarc_sushilogtmp table (sarc_sushilogtmp_json_array)");
        String insert_sarc_sushilogtmp = "INSERT INTO  " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp "
                + "SELECT 'SARC-OJS', split(split(INPUT__FILE__NAME,'SarcsARReport_')[1],'_')[0], "
                + " `ItemIdent`.`Value`, `ItemPerformance`.`Period`.`Begin`, "
                + "`ItemPerformance`.`Instance`.`MetricType`, `ItemPerformance`.`Instance`.`Count` "
                + "FROM " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_json_array "
                + "LATERAL VIEW posexplode(ItemIdentifier) ItemIdentifierTable AS seqi, ItemIdent "
                + "WHERE `ItemIdent`.`Type`='DOI'";
        stmt.executeUpdate(insert_sarc_sushilogtmp);
        logger.info("Inserted to sarc_sushilogtmp table (sarc_sushilogtmp_json_array)");

        logger.info("Inserting to sarc_sushilogtmp table (sarc_sushilogtmp_json_non_array)");
        insert_sarc_sushilogtmp = "INSERT INTO  " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp "
                + "SELECT 'SARC-OJS', split(split(INPUT__FILE__NAME,'SarcsARReport_')[1],'_')[0], "
                + "`ItemIdentifier`.`Value`, `ItemPerformance`.`Period`.`Begin`, "
                + "`ItemPerformance`.`Instance`.`MetricType`, `ItemPerformance`.`Instance`.`Count` "
                + "FROM " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_json_non_array";
        stmt.executeUpdate(insert_sarc_sushilogtmp);
        logger.info("Inserted to sarc_sushilogtmp table (sarc_sushilogtmp_json_non_array)");

        ConnectDB.getHiveConnection().close();
    }

    public void getAndProcessSarc(String sarcsReportPathArray, String sarcsReportPathNonArray) throws Exception {

        Statement stmt = ConnectDB.getHiveConnection().createStatement();
        ConnectDB.getHiveConnection().setAutoCommit(false);

        logger.info("Creating sushilog table");
        String createSushilog = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".sushilog "
                + "(`source` string, "
                + "`repository` string, "
                + "`rid` string, "
                + "`date`	string, "
                + "`metric_type` string,	"
                + "`count`	int)";
        stmt.executeUpdate(createSushilog);
        logger.info("Created sushilog table");

        logger.info("Dropping sarc_sushilogtmp table");
        String drop_sarc_sushilogtmp = "DROP TABLE IF EXISTS "
                + ConnectDB.getUsageStatsDBSchema()
                + ".sarc_sushilogtmp";
        stmt.executeUpdate(drop_sarc_sushilogtmp);
        logger.info("Dropped sarc_sushilogtmp table");
        ConnectDB.getHiveConnection().close();

        List<String[]> issnAndUrls = new ArrayList<String[]>();
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/motricidade/sushiLite/v1_7/", "1646-107X"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/antropologicas/sushiLite/v1_7/", "0873-819X"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/interaccoes/sushiLite/v1_7/", "1646-2335"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/cct/sushiLite/v1_7/", "2182-3030"
        });
        issnAndUrls.add(new String[]{
            "https://actapediatrica.spp.pt/sushiLite/v1_7/", "0873-9781"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/sociologiapp/sushiLite/v1_7/", "0873-6529"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/finisterra/sushiLite/v1_7/", "0430-5027"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/sisyphus/sushiLite/v1_7/", "2182-8474"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/anestesiologia/sushiLite/v1_7/", "0871-6099"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/rpe/sushiLite/v1_7/", "0871-9187"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/psilogos/sushiLite/v1_7/", "1646-091X"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/juridica/sushiLite/v1_7/", "2183-5799"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/ecr/sushiLite/v1_7/", "1647-2098"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/nascercrescer/sushiLite/v1_7/", "0872-0754"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/cea/sushiLite/v1_7/", "1645-3794"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/proelium/sushiLite/v1_7/", "1645-8826"
        });
        issnAndUrls.add(new String[]{
            "https://revistas.rcaap.pt/millenium/sushiLite/v1_7/", "0873-3015"
        });

        if (ExecuteWorkflow.sarcNumberOfIssnToDownload > 0
                && ExecuteWorkflow.sarcNumberOfIssnToDownload <= issnAndUrls.size()) {
            logger.info("Trimming siteIds list to the size of: " + ExecuteWorkflow.sarcNumberOfIssnToDownload);
            issnAndUrls = issnAndUrls.subList(0, ExecuteWorkflow.sarcNumberOfIssnToDownload);
        }

        logger.info("(getAndProcessSarc) Downloading the followins opendoars: " + issnAndUrls);

        for (String[] issnAndUrl : issnAndUrls) {
            logger.info("Now working on ISSN: " + issnAndUrl[1]);
            getARReport(sarcsReportPathArray, sarcsReportPathNonArray, issnAndUrl[0], issnAndUrl[1]);
        }

    }

    public void finalizeSarcStats() throws Exception {
        stmtHive = ConnectDB.getHiveConnection().createStatement();
        ConnectDB.getHiveConnection().setAutoCommit(false);
        stmtImpala = ConnectDB.getImpalaConnection().createStatement();

        logger.info("Creating downloads_stats table_tmp");
        String createDownloadsStats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".downloads_stats_tmp "
                + "(`source` string, "
                + "`repository_id` string, "
                + "`result_id` string, "
                + "`date`	string, "
                + "`count` bigint,	"
                + "`openaire`	bigint)";
        stmtHive.executeUpdate(createDownloadsStats);
        logger.info("Created downloads_stats_tmp table");

        logger.info("Dropping sarc_sushilogtmp_impala table");
        String drop_sarc_sushilogtmp_impala = "DROP TABLE IF EXISTS "
                + ConnectDB.getUsageStatsDBSchema()
                + ".sarc_sushilogtmp_impala";
        stmtHive.executeUpdate(drop_sarc_sushilogtmp_impala);
        logger.info("Dropped sarc_sushilogtmp_impala table");

        logger.info("Creating sarc_sushilogtmp_impala, a table readable by impala");
        String createSarcSushilogtmpImpala = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".sarc_sushilogtmp_impala "
                + "STORED AS PARQUET AS SELECT * FROM " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp";
        stmtHive.executeUpdate(createSarcSushilogtmpImpala);
        logger.info("Created sarc_sushilogtmp_impala");

        logger.info("Making sarc_sushilogtmp visible to impala");
        String invalidateMetadata = "INVALIDATE METADATA " + ConnectDB.getUsageStatsDBSchema()
                + ".sarc_sushilogtmp_impala;";
        stmtImpala.executeUpdate(invalidateMetadata);

        logger.info("Dropping downloads_stats_impala table");
        String drop_downloads_stats_impala = "DROP TABLE IF EXISTS "
                + ConnectDB.getUsageStatsDBSchema()
                + ".downloads_stats_impala";
        stmtHive.executeUpdate(drop_downloads_stats_impala);
        logger.info("Dropped downloads_stats_impala table");

        logger.info("Making downloads_stats_impala deletion visible to impala");
        try {
            String invalidateMetadataDownloadsStatsImpala = "INVALIDATE METADATA " + ConnectDB.getUsageStatsDBSchema()
                    + ".downloads_stats_impala;";
            stmtImpala.executeUpdate(invalidateMetadataDownloadsStatsImpala);
        } catch (SQLException sqle) {
        }

        // We run the following query in Impala because it is faster
        logger.info("Creating downloads_stats_impala");
        String createDownloadsStatsImpala = "CREATE TABLE " + ConnectDB.getUsageStatsDBSchema()
                + ".downloads_stats_impala AS "
                + "SELECT s.source, d.id AS repository_id, "
                + "ro.id as result_id, CONCAT(CAST(YEAR(`date`) AS STRING), '/', "
                + "LPAD(CAST(MONTH(`date`) AS STRING), 2, '0')) AS `date`, s.count, '0' "
                + "FROM " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp_impala s, "
                + ConnectDB.getStatsDBSchema() + ".datasource_oids d, "
                + ConnectDB.getStatsDBSchema() + ".result_pids ro "
                + "WHERE d.oid LIKE CONCAT('%', s.repository, '%') AND d.id like CONCAT('%', 'sarcservicod', '%') "
                + "AND s.rid=ro.pid AND ro.type='Digital Object Identifier' AND s.metric_type='ft_total' AND s.source='SARC-OJS'";
        stmtImpala.executeUpdate(createDownloadsStatsImpala);
        logger.info("Creating downloads_stats_impala");

        // Insert into downloads_stats
        logger.info("Inserting data from downloads_stats_impala into downloads_stats_tmp");
        String insertDStats = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema()
                + ".downloads_stats_tmp SELECT * "
                + "FROM " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats_impala";
        stmtHive.executeUpdate(insertDStats);
        logger.info("Inserted into downloads_stats_tmp");

        logger.info("Creating sushilog table");
        String createSushilog = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".sushilog "
                + "(`source` string, "
                + "`repository_id` string, "
                + "`rid` string, "
                + "`date`	string, "
                + "`metric_type` string,	"
                + "`count`	int)";
        stmtHive.executeUpdate(createSushilog);
        logger.info("Created sushilog table");

        // Insert into sushilog
        logger.info("Inserting into sushilog");
        String insertSushiLog = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema()
                + ".sushilog SELECT * " + "FROM " + ConnectDB.getUsageStatsDBSchema() + ".sarc_sushilogtmp";
        stmtHive.executeUpdate(insertSushiLog);
        logger.info("Inserted into sushilog");

        stmtHive.close();
        ConnectDB.getHiveConnection().close();
    }

    public void getARReport(String sarcsReportPathArray, String sarcsReportPathNonArray,
            String url, String issn) throws Exception {
        logger.info("Processing SARC! issn: " + issn + " with url: " + url);
        ConnectDB.getHiveConnection().setAutoCommit(false);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM");
        // Setting the starting period
        Calendar start = (Calendar) ExecuteWorkflow.startingLogPeriod.clone();
        logger.info("(getARReport) Starting period for log download: " + simpleDateFormat.format(start.getTime()));

        // Setting the ending period (last day of the month)
        Calendar end = (Calendar) ExecuteWorkflow.endingLogPeriod.clone();
        end.add(Calendar.MONTH, +1);
        end.add(Calendar.DAY_OF_MONTH, -1);
        logger.info("(getARReport) Ending period for log download: " + simpleDateFormat.format(end.getTime()));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        PreparedStatement st = ConnectDB
                .getHiveConnection()
                .prepareStatement(
                        "SELECT max(date) FROM " + ConnectDB.getUsageStatsDBSchema() + ".sushilog WHERE repository=?");
        st.setString(1, issn);
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

            // Creating the needed configuration for the correct storing of data
            Configuration config = new Configuration();
            config.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            config
                    .set(
                            "fs.hdfs.impl",
                            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            config
                    .set(
                            "fs.file.impl",
                            org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem dfs = FileSystem.get(config);

        if (dateMax != null && start.getTime().compareTo(dateMax) <= 0) {
            logger.info("Date found in logs " + dateMax + " and not downloanding logs for " + issn);
        } else {
           
            while (start.before(end)) {
                String reportUrl = url + "GetReport/?Report=AR1&Format=json&BeginDate="
                        + simpleDateFormat.format(start.getTime()) + "&EndDate=" + simpleDateFormat.format(start.getTime());
                start.add(Calendar.MONTH, 1);

                logger.info("(getARReport) Getting report: " + reportUrl);
                String text = getJson(reportUrl);
                if (text == null) {
                    continue;
                }

                JSONParser parser = new JSONParser();
                JSONObject jsonObject = null;
                try {
                    jsonObject = (JSONObject) parser.parse(text);
                } // if there is a parsing error continue with the next url
                catch (ParseException pe) {
                    continue;
                }

                jsonObject = (JSONObject) jsonObject.get("sc:ReportResponse");
                jsonObject = (JSONObject) jsonObject.get("sc:Report");
                if (jsonObject == null) {
                    continue;
                }
                jsonObject = (JSONObject) jsonObject.get("c:Report");
                jsonObject = (JSONObject) jsonObject.get("c:Customer");
                Object obj = jsonObject.get("c:ReportItems");
                JSONArray jsonArray = new JSONArray();
                if (obj instanceof JSONObject) {
                    jsonArray.add(obj);
                } else {
                    jsonArray = (JSONArray) obj;
                    // jsonArray = (JSONArray) jsonObject.get("c:ReportItems");
                }
                if (jsonArray == null) {
                    continue;
                }

                // Creating the file in the filesystem for the ItemIdentifier as array object
                String filePathArray = sarcsReportPathArray + "/SarcsARReport_" + issn + "_"
                        + simpleDateFormat.format(start.getTime()) + ".json";
                logger.info("Storing to file: " + filePathArray);
                FSDataOutputStream finArray = dfs.create(new Path(filePathArray), true);

                // Creating the file in the filesystem for the ItemIdentifier as array object
                String filePathNonArray = sarcsReportPathNonArray + "/SarcsARReport_" + issn + "_"
                        + simpleDateFormat.format(start.getTime()) + ".json";
                logger.info("Storing to file: " + filePathNonArray);
                FSDataOutputStream finNonArray = dfs.create(new Path(filePathNonArray), true);

                for (Object aJsonArray : jsonArray) {

                    JSONObject jsonObjectRow = (JSONObject) aJsonArray;
                    renameKeysRecursively(":", jsonObjectRow);

                    if (jsonObjectRow.get("ItemIdentifier") instanceof JSONObject) {
                        finNonArray.write(jsonObjectRow.toJSONString().getBytes());
                        finNonArray.writeChar('\n');
                    } else {
                        finArray.write(jsonObjectRow.toJSONString().getBytes());
                        finArray.writeChar('\n');
                    }
                }

                finArray.close();
                finNonArray.close();

                // Check the file size and if it is too big, delete it
                File fileArray = new File(filePathArray);
			if (fileArray.length() == 0)
                    fileArray.delete();
                File fileNonArray = new File(filePathNonArray);
			if (fileNonArray.length() == 0)
                    fileNonArray.delete();

            }

            dfs.close();
        }
        //ConnectDB.getHiveConnection().close();
    }

    private void renameKeysRecursively(String delimiter, JSONArray givenJsonObj) throws Exception {
        for (Object jjval : givenJsonObj) {
            if (jjval instanceof JSONArray) {
                renameKeysRecursively(delimiter, (JSONArray) jjval);
            } else if (jjval instanceof JSONObject) {
                renameKeysRecursively(delimiter, (JSONObject) jjval);
            } // All other types of vals
            else
				;
        }
    }

    private void renameKeysRecursively(String delimiter, JSONObject givenJsonObj) throws Exception {
        Set<String> jkeys = new HashSet<String>(givenJsonObj.keySet());
        for (String jkey : jkeys) {

            String[] splitArray = jkey.split(delimiter);
            String newJkey = splitArray[splitArray.length - 1];

            Object jval = givenJsonObj.get(jkey);
            givenJsonObj.remove(jkey);
            givenJsonObj.put(newJkey, jval);

            if (jval instanceof JSONObject) {
                renameKeysRecursively(delimiter, (JSONObject) jval);
            }

            if (jval instanceof JSONArray) {
                renameKeysRecursively(delimiter, (JSONArray) jval);
            }
        }
    }

    private String getJson(String url) throws Exception {
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

            // Logging error and silently continuing
            logger.error("Failed to get URL: " + e);
            System.out.println("Failed to get URL: " + e);
//			return null;
//			throw new Exception("Failed to get URL: " + e.toString(), e);
        }
        return "";
    }
}
