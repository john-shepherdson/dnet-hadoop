/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.dnetlib.oa.graph.datasetsusagestats.export;

import java.io.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author D.Pierrakos
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
		ArrayList<String> jsonFiles = listHdfsDir(dataciteReportPath);
		for (String jsonFile : jsonFiles) {
			logger.info("Reading report file " + jsonFile);
			this.createTmpReportsTable(jsonFile);

			String sqlSelectReportID = "SELECT get_json_object(json, '$.report.id')  FROM "
				+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsonToTable";
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
					+ "FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsonToTable";
				stmt.execute(sqlInsertReport);

				logger.info("Report added");

				logger.info("Adding datasets");
				String sqlSelecteDatasetsArray = "SELECT get_json_object(json, '$.report.report-datasets')  FROM "
					+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsonToTable";
				stmt.execute(sqlSelecteDatasetsArray);
				ResultSet rstmpReportDatasets = stmt.getResultSet();

				if (rstmpReportDatasets.next() && rstmpReportDatasets.getString(1).indexOf(',') > 0) {
					// String[] listDatasets = rstmpReportDatasets.getString(1).split(",");
					// String listDatasets = rstmpReportDatasets.getString(1);
					String sqlSelectReport = "SELECT * FROM "
						+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsonToTable";
					stmt.execute(sqlSelectReport);
					ResultSet rstmpReportAll = stmt.getResultSet();
					if (rstmpReportAll.next()) {
						String listDatasets = rstmpReportAll.getString(1);
						logger.info("Adding uncompressed performance for " + reportID);
						this.readDatasetsReport(listDatasets, reportID);
					}

				}
				logger.info("Adding gziped performance for datasets");
				String sqlSelecteReportSubsets = "SELECT get_json_object(json, '$.report.report-subsets.gzip[0]')  FROM "
					+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsonToTable";
				stmt.execute(sqlSelecteReportSubsets);
				ResultSet rstmpReportSubsets = stmt.getResultSet();
				if (rstmpReportSubsets.next()) {
					String unCompressedReport = uncompressString(rstmpReportSubsets.getString(1));
					this.readDatasetsReport(unCompressedReport, reportID);
				}
			}
		}
		this.dropTmpReportsTable();
	}

	public void readDatasetsReport(String prettyDatasetsReports, String reportId) throws Exception {
		logger.info("Reading Datasets performance for report " + reportId);
		logger.info("Write Performance Report To File");

		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readValue(prettyDatasetsReports, JsonNode.class);
		String datasetsReports = jsonNode.toString();
		String report = datasetsReports
			.replace("report-datasets", "report_datasets")
			.replace("dataset-title", "dataset_title")
			.replace("dataset-id", "dataset_id")
			.replace("data-type", "data_type")
			.replace("publisher-id", "publisher_id")
			.replace("dataset-contributors", "dataset_contributors")
			.replace("begin-date", "begin_date")
			.replace("end-date", "end_date")
			.replace("access-method", "access_method")
			.replace("metric-type", "metric_type")
			.replace("doi:", "");
		FileSystem fs = FileSystem.get(new Configuration());
		String tmpPath = dataciteReportPath + "/tmpjson";
		FSDataOutputStream fin = fs
			.create(new Path(dataciteReportPath + "/tmpjson/" + reportId + "_Compressed.json"), true);
		byte[] jsonObjectRawBytes = report.getBytes();

		fin.write(jsonObjectRawBytes);

		fin.writeChar('\n');
		fin.close();

		logger.info("Reading Performance Report From File...");

		String sqlCreateTempTableForDatasets = "CREATE TEMPORARY TABLE " + ConnectDB.getDataSetUsageStatsDBSchema()
			+ ".tmpjsoncompressesed (report_datasets array<struct<dataset_id:array<struct<value:string>>,dataset_title:string, data_type:string, "
			+ "uri:string, publisher:string, publisher_id:array<struct<type:string, value:string>>,platform:string, yop:string, "
			+ "dataset_contributors:array<struct<type:string, value:string>>,"
			+ "performance:array<struct<period:struct<begin_date:string,end_date:string>, "
			+ "instance:array<struct<count:int,access_method:string,metric_type:string>>>>>>) "
			+ "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n"
			+ "LOCATION '" + tmpPath + "'";

		Statement stmt = ConnectDB.getHiveConnection().createStatement();

		ConnectDB.getHiveConnection().setAutoCommit(false);

		logger.info("Adding JSON Serde jar");
		stmt.executeUpdate("add jar /usr/share/cmf/common_jars/hive-hcatalog-core-1.1.0-cdh5.14.0.jar");
		logger.info("Added JSON Serde jar");

		logger.info("Inserting Datasets Performance");
		stmt.execute(sqlCreateTempTableForDatasets);

		String sqlInsertToDatasetsPerformance = "INSERT INTO " + ConnectDB.getDataSetUsageStatsDBSchema()
			+ ".datasetsperformance SELECT dataset.dataset_id[0].value ds_type, "
			+ " dataset.dataset_title ds_title, "
			+ " dataset.yop yop, "
			+ " dataset.data_type dataset_type, "
			+ " dataset.uri uri, "
			+ " dataset.platform platform, "
			+ " dataset.publisher publisher, "
			+ " dataset.publisher_id publisher_id, "
			+ " dataset.dataset_contributors dataset_contributors, "
			+ " period.end_date period_end, "
			+ " period.begin_date period_from, "
			+ " performance.access_method access_method, "
			+ " performance.metric_type metric_type, "
			+ " performance.count count, "
			+ "'" + reportId + "' report_id "
			+ " FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsoncompressesed "
			+ " LATERAL VIEW explode(report_datasets) exploded_table as dataset LATERAL VIEW explode(dataset.performance[0].instance) exploded_table2 as performance "
			+ " LATERAL VIEW explode (array(dataset.performance[0].period)) exploded_table3 as period";

		stmt.executeUpdate(sqlInsertToDatasetsPerformance);

		logger.info("Datasets Performance Inserted for Report " + reportId);

		stmt.execute("Drop table " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsoncompressesed");

		logger.info("Datasets Report Added");

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
			// uncompressedReport = sb.toString().replace("][{\"idSite\"", ",{\"idSite\"");
			result = sb.toString().trim();
			// fs.close();
		} catch (Exception e) {
			throw new Exception(e);
		}

		return result;
	}

	public static String uncompressString(String zippedBase64Str)
		throws IOException {
		String uncompressedReport = null;

		byte[] bytes = Base64.getDecoder().decode(zippedBase64Str);
		GZIPInputStream zi = null;
		try {
			zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
			uncompressedReport = IOUtils.toString(zi);
		} finally {
			IOUtils.closeQuietly(zi);
		}
		logger.info("Report Succesfully Uncompressed...");
		return uncompressedReport;
	}

	private void createTmpReportsTable(String jsonFile) throws SQLException {
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		dropTmpReportsTable();
		String createTmpTable = "CREATE TEMPORARY TABLE " + ConnectDB.getDataSetUsageStatsDBSchema()
			+ ".tmpjsonToTable (json STRING)";
		stmt.executeUpdate(createTmpTable);
		logger.info("Temporary Table for Json Report Created");

		String insertJsonReport = "LOAD DATA INPATH '" + jsonFile + "' INTO TABLE "
			+ ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsonToTable";
		stmt.execute(insertJsonReport);
		logger.info("JSON Report File inserted to tmpjsonToTable Table");
	}

	private void dropTmpReportsTable() throws SQLException {
		logger.info("Dropping tmpjson Table");
		String dropTmpTable = "DROP TABLE IF EXISTS " + ConnectDB.getDataSetUsageStatsDBSchema() + ".tmpjsonToTable";
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		stmt.executeUpdate(dropTmpTable);
		logger.info("Dropped Table for Json Report Table");

	}

	public void createUsageStatisticsTable() throws SQLException {
		logger.info("Dropping Downloads Stats table");
		Statement stmt = ConnectDB.getHiveConnection().createStatement();
		String dropDownloadsTable = "DROP TABLE IF EXISTS " + ConnectDB.getDataSetUsageStatsDBSchema()
			+ ".datacite_downloads";
		stmt.executeUpdate(dropDownloadsTable);

		logger.info("Creating Downloads Stats table");
		String createDownloadsTable = "CREATE TABLE " + ConnectDB.getDataSetUsageStatsDBSchema()
			+ ".datacite_downloads as "
			+ "SELECT 'Datacite' source, d.id repository_id, od.id result_id, regexp_replace(substring(string(period_end),0,7),'-','/') date, count, '0' openaire "
			+ "FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".datasetsperformance "
			+ "JOIN " + ConnectDB.getStatsDBSchema() + ".datasource d on name=platform "
			+ "JOIN " + ConnectDB.getStatsDBSchema() + ".result_oids od on string(ds_type)=od.oid "
			+ "where metric_type='total-dataset-requests'";
		stmt.executeUpdate(createDownloadsTable);
		logger.info("Downloads Stats table created");

		logger.info("Creating Views Stats table");
		String createViewsTable = "CREATE TABLE " + ConnectDB.getDataSetUsageStatsDBSchema() + ".datacite_views as "
			+ "SELECT 'Datacite' source, d.id repository_id, od.id result_id, regexp_replace(substring(string(period_end),0,7),'-','/') date, count, '0' openaire "
			+ "FROM " + ConnectDB.getDataSetUsageStatsDBSchema() + ".datasetsperformance "
			+ "JOIN " + ConnectDB.getStatsDBSchema() + ".datasource d on name=platform "
			+ "JOIN " + ConnectDB.getStatsDBSchema() + ".result_oids od on string(ds_type)=od.oid "
			+ "where metric_type='total-dataset-investigations'";
		stmt.executeUpdate(createViewsTable);
		logger.info("Views Stats table created");
	}

}
