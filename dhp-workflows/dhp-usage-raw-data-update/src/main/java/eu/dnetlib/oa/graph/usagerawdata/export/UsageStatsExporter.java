
package eu.dnetlib.oa.graph.usagerawdata.export;

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

		PiwikStatsDB piwikstatsdb = new PiwikStatsDB(ExecuteWorkflow.repoLogPath, ExecuteWorkflow.portalLogPath);

		logger.info("Re-creating database and tables");
		if (ExecuteWorkflow.recreateDbAndTables) {
			piwikstatsdb.recreateDBAndTables();
			logger.info("DB-Tables-TmpTables are created ");
		}

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
			sarcStats.updateSarcLogs();
		}
		logger.info("Sarc done");
		// finalize usagestats

		logger.info("Dropping tmp tables");
		if (ExecuteWorkflow.finalizeStats) {
			piwikstatsdb.finalizeStats();
			logger.info("Dropped tmp tables");
		}

		logger.info("Raw Data Download End");
	}

	public void createdDBWithTablesOnly() throws Exception {
		logger.info("Initialising DB properties");
		ConnectDB.init();

		PiwikStatsDB piwikstatsdb = new PiwikStatsDB(ExecuteWorkflow.repoLogPath, ExecuteWorkflow.portalLogPath);
		piwikstatsdb.recreateDBAndTables();

		piwikstatsdb.createPedocsOldUsageData();
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

		logger.info("Creating sushilog");

		String sqlCreateTableSushiLog = "CREATE TABLE IF NOT EXISTS " + ConnectDB.getUsageStatsDBSchema()
			+ ".sushilog(source STRING, "
			+ "repository STRING, rid STRING, date STRING, metric_type STRING, count INT)  clustered by (source, "
			+ "repository, rid, date, metric_type) into 100 buckets stored as orc tblproperties('transactional'='true')";
		stmt.executeUpdate(sqlCreateTableSushiLog);
		logger.info("Created sushilog");

		logger.info("Updating piwiklog");
		String sql = "insert into " + ConnectDB.getUsageStatsDBSchema()
			+ ".piwiklog select * from openaire_prod_usage_raw.piwiklog";
		stmt.executeUpdate(sql);

		logger.info("Updating lareferencialog");
		sql = "insert into " + ConnectDB.getUsageStatsDBSchema()
			+ ".lareferencialog select * from openaire_prod_usage_raw.lareferencialog";
		stmt.executeUpdate(sql);

		logger.info("Updating sushilog");
		sql = "insert into " + ConnectDB.getUsageStatsDBSchema()
			+ ".sushilog select * from openaire_prod_usage_raw.sushilog";
		stmt.executeUpdate(sql);

		stmt.close();
		ConnectDB.getHiveConnection().close();
		logger.info("Sushi Tables Created");

	}

}
