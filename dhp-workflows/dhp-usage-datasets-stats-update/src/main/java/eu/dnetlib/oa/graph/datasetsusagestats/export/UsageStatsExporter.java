package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for downloading and processing Usage statistics
 *
 * @author D. Pierrakos, S. Zoupanos
 */
public class UsageStatsExporter {

    private Statement stmt = null;

    public UsageStatsExporter() {

    }

    private static final Logger logger = LoggerFactory.getLogger(UsageStatsExporter.class);

    private void reCreateLogDirs() throws IllegalArgumentException, IOException {
        FileSystem dfs = FileSystem.get(new Configuration());

        logger.info("Deleting Log directory: " + ExecuteWorkflow.dataciteReportPath);
        dfs.delete(new Path(ExecuteWorkflow.dataciteReportPath), true);

        logger.info("Creating Log directory: " + ExecuteWorkflow.dataciteReportPath);
        dfs.mkdirs(new Path(ExecuteWorkflow.dataciteReportPath));

    }

    public void export() throws Exception {

        logger.info("Initialising DB properties");
        ConnectDB.init();
        ConnectDB.getHiveConnection();

        if (ExecuteWorkflow.recreateDbAndTables) {
            createDatabase();
            createTables();
            reCreateLogDirs();
        }
        logger.info("Initializing the download logs module");
        DownloadReportsListFromDatacite drfd = new DownloadReportsListFromDatacite(ExecuteWorkflow.dataciteBaseURL, ExecuteWorkflow.dataciteReportPath);

        if (ExecuteWorkflow.datasetsEmptyDirs) {
            logger.info("Downloading Reports List From Datacite");
            drfd.downloadReportsList();
            logger.info("Reports List has been downloaded");
        }
    }

    private void createDatabase() throws Exception {
        try {
            stmt = ConnectDB.getHiveConnection().createStatement();

            logger.info("Dropping datasetUsageStats DB: " + ConnectDB.getDataSetUsageStatsDBSchema());
            String dropDatabase = "DROP DATABASE IF EXISTS " + ConnectDB.getDataSetUsageStatsDBSchema() + " CASCADE";
            stmt.executeUpdate(dropDatabase);
        } catch (Exception e) {
            logger.error("Failed to drop database: " + e);
            throw new Exception("Failed to drop database: " + e.toString(), e);
        }

        try {
            stmt = ConnectDB.getHiveConnection().createStatement();

            logger.info("Creating datasetUsageStats DB: " + ConnectDB.getDataSetUsageStatsDBSchema());
            String createDatabase = "CREATE DATABASE IF NOT EXISTS " + ConnectDB.getDataSetUsageStatsDBSchema();
            stmt.executeUpdate(createDatabase);

        } catch (Exception e) {
            logger.error("Failed to create database: " + e);
            throw new Exception("Failed to create database: " + e.toString(), e);
        }
    }

    private void createTables() throws Exception {
        try {
            stmt = ConnectDB.getHiveConnection().createStatement();

            // Create Reports table - This table should exist
            String sqlCreateTableDataciteeReports = "CREATE TABLE IF NOT EXISTS "
                    + ConnectDB.getDataSetUsageStatsDBSchema()
                    + ".datacitereports(reportid STRING, \n"
                    + "	name STRING, \n"
                    + "    source STRING,\n"
                    + "    release STRING,\n"
                    + "    createdby STRING,\n"
                    + "    report_end_date STRING,\n"
                    + "    report_start_date STRING)\n"
                    + "    CLUSTERED BY (reportid)\n"
                    + "	into 100 buckets stored as orc tblproperties('transactional'='true')";

            stmt.executeUpdate(sqlCreateTableDataciteeReports);

            stmt.close();
            ConnectDB.getHiveConnection().close();

        } catch (Exception e) {
            logger.error("Failed to create tables: " + e);
            throw new Exception("Failed to create tables: " + e.toString(), e);
        }
    }

//		runImpalaQuery();
/*
		PiwikStatsDB piwikstatsdb = new PiwikStatsDB(ExecuteWorkflow.repoLogPath, ExecuteWorkflow.portalLogPath);

		logger.info("Re-creating database and tables");

		logger.info("Initializing the download logs module");
		PiwikDownloadLogs piwd = new PiwikDownloadLogs(ExecuteWorkflow.matomoBaseURL, ExecuteWorkflow.matomoAuthToken);

		if (ExecuteWorkflow.piwikEmptyDirs) {
			logger.info("Recreating Piwik log directories");
			piwikstatsdb.reCreateLogDirs();
		}

		// Downloading piwik logs (also managing directory creation)
		if (ExecuteWorkflow.downloadPiwikLogs) {
			logger.info("Downloading piwik logs");
			piwd
				.GetOpenAIRELogs(
					ExecuteWorkflow.repoLogPath,
					ExecuteWorkflow.portalLogPath, ExecuteWorkflow.portalMatomoID);
		}
		logger.info("Downloaded piwik logs");

		// Create DB tables, insert/update statistics
		String cRobotsUrl = "https://raw.githubusercontent.com/atmire/COUNTER-Robots/master/COUNTER_Robots_list.json";
		piwikstatsdb.setCounterRobotsURL(cRobotsUrl);

		if (ExecuteWorkflow.processPiwikLogs) {
			logger.info("Processing logs");
			piwikstatsdb.processLogs();
		}

		logger.info("Creating LaReferencia tables");
		LaReferenciaDownloadLogs lrf = new LaReferenciaDownloadLogs(ExecuteWorkflow.lareferenciaBaseURL,
			ExecuteWorkflow.lareferenciaAuthToken);

		if (ExecuteWorkflow.laReferenciaEmptyDirs) {
			logger.info("Recreating LaReferencia log directories");
			lrf.reCreateLogDirs();
		}

		if (ExecuteWorkflow.downloadLaReferenciaLogs) {
			logger.info("Downloading LaReferencia logs");
			lrf.GetLaReferenciaRepos(ExecuteWorkflow.lareferenciaLogPath);
			logger.info("Downloaded LaReferencia logs");
		}
		LaReferenciaStats lastats = new LaReferenciaStats(ExecuteWorkflow.lareferenciaLogPath);

		if (ExecuteWorkflow.processLaReferenciaLogs) {
			logger.info("Processing LaReferencia logs");
			lastats.processLogs();
			logger.info("LaReferencia logs done");
		}

		IrusStats irusstats = new IrusStats(ExecuteWorkflow.irusUKBaseURL);
		if (ExecuteWorkflow.irusCreateTablesEmptyDirs) {
			logger.info("Creating Irus Stats tables");
			irusstats.createTables();
			logger.info("Created Irus Stats tables");

			logger.info("Re-create log dirs");
			irusstats.reCreateLogDirs();
			logger.info("Re-created log dirs");
		}

		if (ExecuteWorkflow.irusDownloadReports) {
			irusstats.getIrusRRReport(ExecuteWorkflow.irusUKReportPath);
		}
		if (ExecuteWorkflow.irusProcessStats) {
			irusstats.processIrusStats();
			logger.info("Irus done");
		}

		SarcStats sarcStats = new SarcStats();
		if (ExecuteWorkflow.sarcCreateTablesEmptyDirs) {
			sarcStats.reCreateLogDirs();
		}
		if (ExecuteWorkflow.sarcDownloadReports) {
			sarcStats.getAndProcessSarc(ExecuteWorkflow.sarcsReportPathArray, ExecuteWorkflow.sarcsReportPathNonArray);
		}
		if (ExecuteWorkflow.sarcProcessStats) {
			sarcStats.processSarc(ExecuteWorkflow.sarcsReportPathArray, ExecuteWorkflow.sarcsReportPathNonArray);
			sarcStats.finalizeSarcStats();
		}
		logger.info("Sarc done");

		// finalize usagestats
		if (ExecuteWorkflow.finalizeStats) {
			piwikstatsdb.finalizeStats();
			logger.info("Finalized stats");
		}

		// Make the tables available to Impala
		if (ExecuteWorkflow.finalTablesVisibleToImpala) {
			logger.info("Making tables visible to Impala");
			invalidateMetadata();
		}

		logger.info("End");
     */
}
/*
	private void invalidateMetadata() throws SQLException {
		Statement stmt = null;

		stmt = ConnectDB.getImpalaConnection().createStatement();

		String sql = "INVALIDATE METADATA " + ConnectDB.getDataSetUsageStatsDBSchema() + ".downloads_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getDataSetUsageStatsDBSchema() + ".views_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getDataSetUsageStatsDBSchema() + ".usage_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getDataSetUsageStatsDBSchema() + ".pageviews_stats";
		stmt.executeUpdate(sql);

		stmt.close();
		ConnectDB.getHiveConnection().close();
	}
 */
