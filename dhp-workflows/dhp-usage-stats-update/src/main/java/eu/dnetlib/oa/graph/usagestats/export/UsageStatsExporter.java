
package eu.dnetlib.oa.graph.usagestats.export;

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

	public UsageStatsExporter() {

	}

	private static final Logger logger = LoggerFactory.getLogger(UsageStatsExporter.class);

	private void reCreateLogDirs() throws IllegalArgumentException, IOException {
		FileSystem dfs = FileSystem.get(new Configuration());

		logger.info("Deleting repoLog directory: " + ExecuteWorkflow.repoLogPath);
		dfs.delete(new Path(ExecuteWorkflow.repoLogPath), true);

		logger.info("Deleting portalLog directory: " + ExecuteWorkflow.portalLogPath);
		dfs.delete(new Path(ExecuteWorkflow.portalLogPath), true);

		logger.info("Deleting lareferenciaLog directory: " + ExecuteWorkflow.lareferenciaLogPath);
		dfs.delete(new Path(ExecuteWorkflow.lareferenciaLogPath), true);

		logger.info("Creating repoLog directory: " + ExecuteWorkflow.repoLogPath);
		dfs.mkdirs(new Path(ExecuteWorkflow.repoLogPath));

		logger.info("Creating portalLog directory: " + ExecuteWorkflow.portalLogPath);
		dfs.mkdirs(new Path(ExecuteWorkflow.portalLogPath));

		logger.info("Creating lareferenciaLog directory: " + ExecuteWorkflow.lareferenciaLogPath);
		dfs.mkdirs(new Path(ExecuteWorkflow.lareferenciaLogPath));
	}

	public void export() throws Exception {

		logger.info("Initialising DB properties");
		ConnectDB.init();

//		runImpalaQuery();

		PiwikStatsDB piwikstatsdb = new PiwikStatsDB(ExecuteWorkflow.repoLogPath, ExecuteWorkflow.portalLogPath);

		logger.info("Re-creating database and tables");
		if (ExecuteWorkflow.recreateDbAndTables)
			piwikstatsdb.recreateDBAndTables();
		;

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
	}

	private void invalidateMetadata() throws SQLException {
		Statement stmt = null;

		stmt = ConnectDB.getImpalaConnection().createStatement();

		String sql = "INVALIDATE METADATA " + ConnectDB.getUsageStatsDBSchema() + ".downloads_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getUsageStatsDBSchema() + ".views_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getUsageStatsDBSchema() + ".usage_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getUsageStatsDBSchema() + ".pageviews_stats";
		stmt.executeUpdate(sql);

		stmt.close();
		ConnectDB.getHiveConnection().close();
	}
}
