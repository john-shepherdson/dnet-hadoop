package eu.dnetlib.oa.graph.usagestatsbuild.export;

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

    public IrusStats() throws Exception {
    }

      
    public void processIrusStats() throws Exception {
        Statement stmt = ConnectDB.getHiveConnection().createStatement();
        ConnectDB.getHiveConnection().setAutoCommit(false);


        logger.info("Creating irus_downloads_stats_tmp table");
        String createDownloadsStats = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
                + ".irus_downloads_stats_tmp "
                + "(`source` string, "
                + "`repository_id` string, "
                + "`result_id` string, "
                + "`date`	string, "
                + "`count` bigint,	"
                + "`openaire`	bigint)";
        stmt.executeUpdate(createDownloadsStats);
        logger.info("Created irus_downloads_stats_tmp table");

        logger.info("Inserting into irus_downloads_stats_tmp");
        String insertDStats = "INSERT INTO " + ConnectDB.getUsageStatsDBSchema() + ".irus_downloads_stats_tmp "
                + "SELECT s.source, d.id AS repository_id, "
                + "ro.id as result_id, CONCAT(YEAR(date), '/', LPAD(MONTH(date), 2, '0')) as date, s.count, '0' "
                + "FROM " + ConnectDB.getUsageRawDataDBSchema() + ".sushilog s, "
                + ConnectDB.getStatsDBSchema() + ".datasource_oids d, "
                + ConnectDB.getStatsDBSchema() + ".result_oids ro "
                + "WHERE s.repository=d.oid AND s.rid=ro.oid AND metric_type='ft_total' AND s.source='IRUS-UK'";
        stmt.executeUpdate(insertDStats);
        logger.info("Inserted into irus_downloads_stats_tmp");

        stmt.close();
        //ConnectDB.getHiveConnection().close();
    }

 
}
