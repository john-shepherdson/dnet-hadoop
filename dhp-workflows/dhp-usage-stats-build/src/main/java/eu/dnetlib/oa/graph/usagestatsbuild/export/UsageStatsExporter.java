
package eu.dnetlib.oa.graph.usagestatsbuild.export;

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

	public void export() throws Exception {

		logger.info("Initialising DB properties");
		ConnectDB.init();

		PiwikStatsDB piwikstatsdb = new PiwikStatsDB();

		logger.info("Re-creating database and tables");
		if (ExecuteWorkflow.recreateDbAndTables) {
			piwikstatsdb.recreateDBAndTables();
			logger.info("DB-Tables are created ");
		}

		if (ExecuteWorkflow.processPiwikLogs) {
			logger.info("Processing Piwik logs");
			piwikstatsdb.processLogs();
			logger.info("Piwik logs Done");
			logger.info("Processing Pedocs Old Stats");
			piwikstatsdb.uploadOldPedocs();
			logger.info("Processing Pedocs Old Stats Done");
			logger.info("Processing TUDELFT Stats");
			piwikstatsdb.uploadTUDELFTStats();
			logger.info("Processing TUDELFT Stats Done");

		}

		if (ExecuteWorkflow.processLaReferenciaLogs) {
			logger.info("Processing LaReferencia logs");
			LaReferenciaStats.processLogs();
			logger.info("LaReferencia logs done");
		}

		if (ExecuteWorkflow.irusProcessStats) {
			logger.info("Processing IRUS");
			IrusStats.processIrusStats();
			logger.info("Irus done");
		}

		if (ExecuteWorkflow.sarcProcessStats) {
			new SarcStats().processSarc();
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

		sql = "INVALIDATE METADATA " + ConnectDB.getUsagestatsPermanentDBSchema() + ".downloads_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getUsagestatsPermanentDBSchema() + ".views_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getUsagestatsPermanentDBSchema() + ".usage_stats";
		stmt.executeUpdate(sql);

		sql = "INVALIDATE METADATA " + ConnectDB.getUsagestatsPermanentDBSchema() + ".pageviews_stats";
		stmt.executeUpdate(sql);

		stmt.close();
		ConnectDB.getHiveConnection().close();
	}
}
