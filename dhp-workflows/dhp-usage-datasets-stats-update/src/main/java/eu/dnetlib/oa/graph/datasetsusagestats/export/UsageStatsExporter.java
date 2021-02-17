
package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.io.IOException;
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

		logger.info("Creating tmp directory: " + ExecuteWorkflow.dataciteReportPath + " " + "/tmpjson/");
		dfs.mkdirs(new Path(ExecuteWorkflow.dataciteReportPath + "/tmpjson/"));

	}

	public void export() throws Exception {

		logger.info("Initialising DB properties");
		ConnectDB.init();
		ConnectDB.getHiveConnection();

		if (ExecuteWorkflow.recreateDbAndTables) {
			DatasetsStatsDB datasetsDB = new DatasetsStatsDB("", "");
			datasetsDB.recreateDBAndTables();
		}
		logger.info("Initializing the download logs module");
		DownloadReportsListFromDatacite downloadReportsListFromDatacite = new DownloadReportsListFromDatacite(
			ExecuteWorkflow.dataciteBaseURL,
			ExecuteWorkflow.dataciteReportPath);

		if (ExecuteWorkflow.datasetsEmptyDirs) {
			logger.info("Downloading Reports List From Datacite");
			this.reCreateLogDirs();
			downloadReportsListFromDatacite.downloadReportsList();
			logger.info("Reports List has been downloaded");
		}

		ReadReportsListFromDatacite readReportsListFromDatacite = new ReadReportsListFromDatacite(
			ExecuteWorkflow.dataciteReportPath);
		logger.info("Store Reports To DB");
		readReportsListFromDatacite.readReports();
		logger.info("Reports Stored To DB");
		readReportsListFromDatacite.createUsageStatisticsTable();
	}
}
