/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.io.*;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author dpie
 */
public class ReadReportsListFromDatacite {

	private String dataciteReportPath;
	private static final Logger logger = LoggerFactory.getLogger(UsageStatsExporter.class);

	public ReadReportsListFromDatacite(String dataciteReportPath) throws MalformedURLException, Exception {

		this.dataciteReportPath = dataciteReportPath;
	}

	public void readReports() throws Exception {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		ConnectDB.getHiveConnection().setAutoCommit(false);
		File folder = new File(dataciteReportPath);
		ArrayList<String> jsonFiles = listHdfsDir(dataciteReportPath);
		for (String jsonFile : jsonFiles) {
			logger.info("Reading report file " + jsonFile);
			this.createTmpReportsTable(jsonFile);

			String sqlSelectReportID = "SELECT get_json_object(json, '$.report.id')  FROM "
				+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
			stmt.execute(sqlSelectReportID);
			ResultSet rstmpReportID = stmt.getResultSet();

			String reportID = null;
			while (rstmpReportID.next()) {
				reportID = rstmpReportID.getString(1);
			}

			logger.info("Checking report with id " + reportID);
			String sqlCheckIfReportExists = "SELECT source FROM " + ConnectDB.getDataSetUsageStatsDBSchema()
				+ ".datacitereports where reportid=?";
			PreparedStatement stGetReportID = ConnectDB.getHiveConnection().prepareStatement(sqlCheckIfReportExists);
			stGetReportID.setString(1, reportID);

			ResultSet rsCheckIfReportExist = stGetReportID.executeQuery();

			if (rsCheckIfReportExist.next()) {
				logger.info("Report found with ID " + reportID);
				dropTmpReportsTable();
			} else {
				String sqlInsertReport = "INSERT INTO " + ConnectDB.getDataSetUsageStatsDBSchema()
					+ " .datacitereports "
					+ "SELECT\n"
					+ "  get_json_object(json, '$.report.id') AS reportid,\n"
					+ "  get_json_object(json, '$.report.report-header.report-name') AS name,\n"
					+ "  get_json_object(json, '$.report.report-header.report-id') AS source,\n"
					+ "  get_json_object(json, '$.report.report-header.release') AS release,\n"
					+ "  get_json_object(json, '$.report.report-header.created-by\') AS createdby,\n"
					+ "  get_json_object(json, '$.report.report-header.reporting-period.begin-date') AS fromdate,\n"
					+ "  get_json_object(json, '$.report.report-header.reporting-period.end-date') AS todate    \n"
					+ "FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
				stmt.execute(sqlInsertReport);

				logger.info("Report added");

				logger.info("Adding datasets");
				String sqlSelecteDatasetsArray = "SELECT get_json_object(json, '$.report.report-datasets')  FROM "
					+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
				stmt.execute(sqlSelecteDatasetsArray);
				ResultSet rstmpReportDatasets = stmt.getResultSet();

				if (rstmpReportDatasets.next() && rstmpReportDatasets.getString(1).indexOf(',') > 0) {
					String[] listDatasets = rstmpReportDatasets.getString(1).split(",");
					logger.info("Datasets found " + listDatasets.length);

					for (int i = 0; i < listDatasets.length; i++) {

						String sqlInsertDataSets = "INSERT INTO " + ConnectDB.getDataSetUsageStatsDBSchema()
							+ " .datasets "
							+ "SELECT\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i
							+ "].dataset-id[0].value') AS ds_type,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i
							+ "].dataset-title') AS ds_title,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i + "].yop') AS yop,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i + "].uri') AS uri,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i + "].platform') AS platform,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i + "].data-type') AS data_type,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i + "].publisher') AS publisher,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i
							+ "].publisher-id.type[0]') AS publisher_id_type,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i
							+ "].publisher-id.value[0]') AS publisher_id_value,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i
							+ "].dataset-dates.type[0]') AS ds_dates_type,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i
							+ "].dataset-dates.value[0]') AS ds_dates_value,\n"
							+ "  get_json_object(json, '$.report.report-datasets[" + i
							+ "].dataset-contributors') AS ds_contributors,\n"
							+ "  get_json_object(json, '$.report.id') AS reportid \n"
							+ "FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
						stmt.execute(sqlInsertDataSets);

						logger.info("Dataset added " + i);

						logger.info("Adding Dataset Performance");
						String sqlSelecteDatasetsPerformance = "SELECT get_json_object(json, '$.report.report-datasets["
							+ i + "].performance')  FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
						stmt.execute(sqlSelecteDatasetsPerformance);
						ResultSet rstmpReportDatasetsPerformance = stmt.getResultSet();
						if (rstmpReportDatasetsPerformance.next()
							&& rstmpReportDatasetsPerformance.getString(1).indexOf(',') > 0) {
							String[] listDatasetsPerformance = rstmpReportDatasetsPerformance.getString(1).split(",");
							logger.info("Datasets Performance found " + listDatasetsPerformance.length);
							for (int j = 0; j < listDatasetsPerformance.length; j++) {
								String sqlSelecteDatasetsPerformanceInstance = "SELECT get_json_object(json, '$.report.report-datasets["
									+ i + "].performance')  FROM " + ConnectDB.getDataSetUsageStatsDBSchema()
									+ ".tmpjson";
								stmt.execute(sqlSelecteDatasetsPerformanceInstance);
								ResultSet rstmpReportDatasetsPerformanceInstance = stmt.getResultSet();
								if (rstmpReportDatasetsPerformanceInstance.next()
									&& rstmpReportDatasetsPerformanceInstance.getString(1).indexOf(',') > 0) {
									String[] listDatasetsPerformanceInstance = rstmpReportDatasetsPerformanceInstance
										.getString(1)
										.split(",");
									logger.info("Datasets Performance found " + listDatasetsPerformanceInstance.length);
									for (int k = 0; k < listDatasetsPerformanceInstance.length; k++) {
										String sqlInsertDataSetsPerformance = "INSERT INTO "
											+ ConnectDB.getDataSetUsageStatsDBSchema() + " .datasetsperformance "
											+ "SELECT\n"
											+ "  get_json_object(json, '$.report.report-datasets[" + i
											+ "].dataset-id[0].value') AS ds_type,\n"
											+ "  get_json_object(json, '$.report.report-datasets[" + i
											+ "].performance[" + j + "].period.end-date') AS period_end,\n"
											+ "  get_json_object(json, '$.report.report-datasets[" + i
											+ "].performance[" + j + "].period.begin-date') AS period_from,\n"
											+ "  get_json_object(json, '$.report.report-datasets[" + i
											+ "].performance[" + j + "].instance[" + k
											+ "].access-method') AS access_method,\n"
											+ "  get_json_object(json, '$.report.report-datasets[" + i
											+ "].performance[" + j + "].instance[" + k
											+ "].metric-type') AS metric_type,\n"
											+ "  get_json_object(json, '$.report.report-datasets[" + i
											+ "].performance[" + j + "].instance[" + k + "].count') AS count,\n"
											+ "  get_json_object(json, '$.report.report-datasets[" + i
											+ "].performance[" + j + "].instance[" + k
											+ "].country-counts') AS country_counts,\n"
											+ "  get_json_object(json, '$.report.id') AS reportid \n"
											+ "FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
										stmt.execute(sqlInsertDataSetsPerformance);
									}
								}
							}
						}
						logger.info("DatasetPerformance added for dataset" + i);
					}
				}
				logger.info("Adding gzip performance");
				String sqlSelecteReportSubsets = "SELECT get_json_object(json, '$.report.report-subsets.gzip[0]')  FROM "
					+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
				stmt.execute(sqlSelecteReportSubsets);
				ResultSet rstmpReportSubsets = stmt.getResultSet();
				if (rstmpReportSubsets.next()) {
					String unCompressedReport = uncompressString(rstmpReportSubsets.getString(1));
					this.readCompressedReport(unCompressedReport, reportID);
				}
			}
		}
		this.dropTmpReportsTable();
	}

	public void readCompressedReport(String report, String reportId) throws Exception {
		Gson gson = new Gson();
		JsonObject jsonObject = gson.fromJson(report, JsonObject.class);

		JsonArray jsonReportDatasets;
		if (jsonObject.getAsJsonArray("report_datasets") != null) {
			jsonReportDatasets = jsonObject.getAsJsonArray("report_datasets");
		} else {
			jsonReportDatasets = jsonObject.getAsJsonArray("report-datasets");
		}

		for (JsonElement datasetElement : jsonReportDatasets) {
			// JsonElement dataset_title = datasetElement.getAsJsonObject().get("dataset-title");
			String dataset_title = datasetElement.getAsJsonObject().get("dataset-title").getAsString();
			String yop = datasetElement.getAsJsonObject().get("yop").getAsString();
			String uri = datasetElement.getAsJsonObject().get("uri").getAsString();
			String platform = datasetElement.getAsJsonObject().get("platform").getAsString();
			String data_type = datasetElement.getAsJsonObject().get("data-type").getAsString();
			String publisher = datasetElement.getAsJsonObject().get("publisher").getAsString();

			JsonArray publisher_id = datasetElement.getAsJsonObject().getAsJsonArray("publisher-id");
			String publisher_id_type = "";
			String publisher_id_value = "";
			for (JsonElement publisher_id_Element : publisher_id) {
				publisher_id_type = publisher_id_Element.getAsJsonObject().get("type").getAsString();
				publisher_id_value = publisher_id_Element.getAsJsonObject().get("value").getAsString();
			}
			JsonArray dataset_days = datasetElement.getAsJsonObject().getAsJsonArray("dataset-dates");
			String ds_dates_type = "";
			String ds_dates_value = "";
			for (JsonElement datasetDaysElement : dataset_days) {
				ds_dates_type = datasetDaysElement.getAsJsonObject().get("type").getAsString();
				ds_dates_value = datasetDaysElement.getAsJsonObject().get("value").getAsString();
			}

			JsonArray datasetContributors = null;
			String ds_contributor_type = "";
			String[] ds_contributor_values = null;
			Array ds_contributor_valuesArr = null;

			if (datasetElement.getAsJsonObject().getAsJsonArray("dataset-contributors") != null) {
				datasetContributors = datasetElement.getAsJsonObject().getAsJsonArray("dataset-contributors");

				JsonArray datasetid = datasetElement.getAsJsonObject().getAsJsonArray("dataset-id");
				String doi = "";
				for (JsonElement datasetIDElement : datasetid)
//System.out.println(datasetIDElement.getAsJsonObject().get("value").getAsString());
				{
					doi = datasetIDElement.getAsJsonObject().get("value").getAsString();
				}

				String sqlInsertDataset = "INSERT INTO " + ConnectDB.getDataSetUsageStatsDBSchema()
					+ " .datasets(ds_type,"
					+ "ds_title,yop,uri,platform,data_type,publisher,publisher_id_type,publisher_id_value,"
					+ "ds_dates_type, ds_dates_value, ds_contributors,reportid) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ";

				PreparedStatement pstmtDataset = ConnectDB.DB_HIVE_CONNECTION.prepareStatement(sqlInsertDataset);

				pstmtDataset.setString(1, doi);
				pstmtDataset.setString(2, dataset_title);
				pstmtDataset.setString(3, yop);
				pstmtDataset.setString(4, uri);
				pstmtDataset.setString(5, platform);
				pstmtDataset.setString(6, data_type);
				pstmtDataset.setString(7, publisher);
				pstmtDataset.setString(8, publisher_id_type);
				pstmtDataset.setString(9, publisher_id_value);
				pstmtDataset.setString(10, ds_dates_type);
				pstmtDataset.setString(11, ds_dates_value);
				pstmtDataset.setString(13, datasetContributors.getAsString());
				pstmtDataset.setString(14, reportId);

				pstmtDataset.execute();
				logger.info("Dataset from compressed report addded " + doi);
				/*
				 * JsonArray performance = datasetElement.getAsJsonObject().getAsJsonArray("performance"); for
				 * (JsonElement performanceElement : performance) { JsonObject period =
				 * performanceElement.getAsJsonObject().getAsJsonObject("period"); String end_date =
				 * period.getAsJsonObject().get("end-date").getAsString(); String begin_date =
				 * period.getAsJsonObject().get("begin-date").getAsString(); JsonArray instance =
				 * performanceElement.getAsJsonObject().getAsJsonArray("instance"); for (JsonElement instanceElement :
				 * instance) { int count = instanceElement.getAsJsonObject().get("count").getAsInt(); JsonObject
				 * country_counts = instanceElement.getAsJsonObject().getAsJsonObject("country-counts"); Set<String>
				 * keys = country_counts.keySet(); String[] country = new String[country_counts.size()]; String[]
				 * country_counts_val = new String[country_counts.size()]; Iterator it2 = keys.iterator(); int j = 0;
				 * while (it2.hasNext()) { country[j] = it2.next().toString(); country_counts_val[j] =
				 * country_counts.get(country[j]).getAsString(); } Array countryArr = conn.createArrayOf("text",
				 * country); Array countrycountsArr = conn.createArrayOf("text", country_counts_val); String metrictype
				 * = instanceElement.getAsJsonObject().get("metric-type").getAsString(); String accessMethod =
				 * instanceElement.getAsJsonObject().get("access-method").getAsString(); String
				 * sqlInsertDatasetPerformance =
				 * "INSERT INTO datasetperformance(ds_type,period_end,period_from,access_method,metric_type,count,country,country_count, reportid) VALUES(?,?,?,?,?,?,?,?,?)"
				 * ; PreparedStatement pstmtDatasetPerformance = conn.prepareStatement(sqlInsertDatasetPerformance);
				 * //System.out.println(begin_date + " " + end_date + " " + doi + " " + metrictype + " " + count);
				 * pstmtDatasetPerformance.setString(1, doi); pstmtDatasetPerformance.setString(2, end_date);
				 * pstmtDatasetPerformance.setString(3, begin_date); pstmtDatasetPerformance.setString(4, accessMethod);
				 * pstmtDatasetPerformance.setString(5, metrictype); pstmtDatasetPerformance.setInt(6, count);
				 * pstmtDatasetPerformance.setArray(7, countryArr); pstmtDatasetPerformance.setArray(8,
				 * countrycountsArr); pstmtDatasetPerformance.setString(9, reportId); pstmtDatasetPerformance.execute();
				 * } }
				 */
			}
		}

	}

	private ArrayList<String> listHdfsDir(String dir) throws Exception {

		FileSystem hdfs = FileSystem.get(new Configuration());
		RemoteIterator<LocatedFileStatus> Files;
		ArrayList<String> fileNames = new ArrayList<>();

		try {
			Path exportPath = new Path(hdfs.getUri() + dir);
			Files = hdfs.listFiles(exportPath, false);
			while (Files.hasNext()) {
				String fileName = Files.next().getPath().toString();
				fileNames.add(fileName);
			}

			hdfs.close();
		} catch (Exception e) {
			logger.error("HDFS file path with exported data does not exist : " + new Path(hdfs.getUri() + dir));
			throw new Exception("HDFS file path with exported data does not exist :   " + dir, e);
		}

		return fileNames;
	}

	private String readHDFSFile(String filename) throws Exception {
		String result;
		try {

			FileSystem fs = FileSystem.get(new Configuration());
			// log.info("reading file : " + filename);

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filename))));

			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				// sb.append(line);
				line = br.readLine();
			}
			// result = sb.toString().replace("][{\"idSite\"", ",{\"idSite\"");
			result = sb.toString().trim();
			// fs.close();
		} catch (Exception e) {
			throw new Exception(e);
		}

		return result;
	}

	public static String uncompressString(String zippedBase64Str)
		throws IOException {
		String result = null;

		// In my solr project, I use org.apache.solr.common.util.Base64.
		// byte[] bytes =
		// org.apache.solr.common.util.Base64.base64ToByteArray(zippedBase64Str);
		byte[] bytes = Base64.getDecoder().decode(zippedBase64Str);
		GZIPInputStream zi = null;
		try {
			zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
			result = IOUtils.toString(zi);
		} finally {
			IOUtils.closeQuietly(zi);
		}
		return result;
	}

	private void createTmpReportsTable(String jsonFile) throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		dropTmpReportsTable();
		String createTmpTable = "CREATE TEMPORARY TABLE " + ConnectDB.getDataSetUsageStatsDBSchema()
			+ ".tmpjson (json STRING)";
		stmt.executeUpdate(createTmpTable);
		logger.info("Tmp Table Created");

		String insertJsonReport = "LOAD DATA INPATH '" + jsonFile + "' INTO TABLE "
			+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
		stmt.execute(insertJsonReport);
		logger.info("JSON Report File inserted to tmpjson Table");
	}

	private void dropTmpReportsTable() throws SQLException {
		logger.info("Dropping tmpjson Table");
		String dropTmpTable = "DROP TABLE IF EXISTS " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjson";
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		stmt.executeUpdate(dropTmpTable);
		logger.info("Dropped tmpjson Table");

	}

}

/*
 * PreparedStatement prepStatem = conn.
 * prepareStatement("insert into usageStats (source, entityID,sourceItemType,entityType, counter,action,timestamp_month,referrer) values (?,?,?,?,?,?,?,?)"
 * );
 */
