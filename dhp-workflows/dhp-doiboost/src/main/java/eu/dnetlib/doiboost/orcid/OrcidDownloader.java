
package eu.dnetlib.doiboost.orcid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class OrcidDownloader extends OrcidDSManager {

	static final int REQ_LIMIT = 24;
//	static final int REQ_MAX_TEST = 100;
	static final int RECORD_PARSED_COUNTER_LOG_INTERVAL = 10000;
	static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	static final String lastUpdate = "2019-09-30 00:00:00";
	private String lambdaFileName;
	private String outputPath;
	private String token;

	public static void main(String[] args) throws IOException, Exception {
		OrcidDownloader orcidDownloader = new OrcidDownloader();
		orcidDownloader.loadArgs(args);
		orcidDownloader.parseLambdaFile();
	}

	private String downloadRecord(String orcidId) {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", String.format("Bearer %s", token));
			CloseableHttpResponse response = client.execute(httpGet);
			if (response.getStatusLine().getStatusCode() != 200) {
				Log
					.warn(
						"Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
				return new String("");
			}
			return IOUtils.toString(response.getEntity().getContent());

		} catch (Throwable e) {
			Log.warn("Downloading " + orcidId, e.getMessage());

		}
		return new String("");
	}

	public void parseLambdaFile() throws Exception {
		int parsedRecordsCounter = 0;
		int downloadedRecordsCounter = 0;
		int savedRecordsCounter = 0;
		long startDownload = 0;
		Configuration conf = initConfigurationObject();
		FileSystem fs = initFileSystemObject(conf);
		String lambdaFileUri = hdfsServerUri.concat(workingPath).concat(lambdaFileName);
		Path hdfsreadpath = new Path(lambdaFileUri);
		FSDataInputStream lambdaFileStream = fs.open(hdfsreadpath);
		Path hdfsoutputPath = new Path(
			hdfsServerUri
				.concat(workingPath)
				.concat(outputPath)
				.concat("orcid_records.seq"));

		try (SequenceFile.Writer writer = SequenceFile
			.createWriter(
				conf,
				SequenceFile.Writer.file(hdfsoutputPath),
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class))) {

			try (BufferedReader br = new BufferedReader(new InputStreamReader(lambdaFileStream))) {
				String line;
				int nReqTmp = 0;
				startDownload = System.currentTimeMillis();
				long startReqTmp = System.currentTimeMillis();
				while ((line = br.readLine()) != null) {
					parsedRecordsCounter++;
					// skip headers line
					if (parsedRecordsCounter == 1) {
						continue;
					}
					String[] values = line.split(",");
					List<String> recordInfo = Arrays.asList(values);
					String orcidId = recordInfo.get(0);
					if (isModified(orcidId, recordInfo.get(3))) {
						String record = downloadRecord(orcidId);
						downloadedRecordsCounter++;
						if (!record.isEmpty()) {
							String compressRecord = ArgumentApplicationParser.compressArgument(record);
							final Text key = new Text(recordInfo.get(0));
							final Text value = new Text(compressRecord);

							try {
								writer.append(key, value);
								savedRecordsCounter++;
							} catch (IOException e) {
								Log.warn("Writing to sequence file: " + e.getMessage());
								Log.warn(e);
								throw new RuntimeException(e);
							}
						}
					}
					long endReq = System.currentTimeMillis();
					nReqTmp++;
					if (nReqTmp == REQ_LIMIT) {
						long reqSessionDuration = endReq - startReqTmp;
						if (reqSessionDuration <= 1000) {
							Log
								.warn(
									"\nreqSessionDuration: "
										+ reqSessionDuration
										+ " nReqTmp: "
										+ nReqTmp
										+ " wait ....");
							Thread.sleep(1000 - reqSessionDuration);
						} else {
							nReqTmp = 0;
							startReqTmp = System.currentTimeMillis();
						}
					}

//					if (parsedRecordsCounter > REQ_MAX_TEST) {
//						break;
//					}
					if ((parsedRecordsCounter % RECORD_PARSED_COUNTER_LOG_INTERVAL) == 0) {
						Log
							.info(
								"Current parsed: "
									+ parsedRecordsCounter
									+ " downloaded: "
									+ downloadedRecordsCounter
									+ " saved: "
									+ savedRecordsCounter);
//						if (parsedRecordsCounter > REQ_MAX_TEST) {
//							break;
//						}
					}
				}
				long endDownload = System.currentTimeMillis();
				long downloadTime = endDownload - startDownload;
				Log.info("Download time: " + ((downloadTime / 1000) / 60) + " minutes");
			}
		}
		lambdaFileStream.close();
		Log.info("Download started at: " + new Date(startDownload).toString());
		Log.info("Parsed Records Counter: " + parsedRecordsCounter);
		Log.info("Downloaded Records Counter: " + downloadedRecordsCounter);
		Log.info("Saved Records Counter: " + savedRecordsCounter);
	}

	private void loadArgs(String[] args) throws IOException, Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					OrcidDownloader.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);

		hdfsServerUri = parser.get("hdfsServerUri");
		Log.info("HDFS URI: " + hdfsServerUri);
		workingPath = parser.get("workingPath");
		Log.info("Default Path: " + workingPath);
		lambdaFileName = parser.get("lambdaFileName");
		Log.info("Lambda File Name: " + lambdaFileName);
		outputPath = parser.get("outputPath");
		Log.info("Output Data: " + outputPath);
		token = parser.get("token");
	}

	private boolean isModified(String orcidId, String modifiedDate) {
		Date modifiedDateDt = null;
		Date lastUpdateDt = null;
		try {
			if (modifiedDate.length() != 19) {
				modifiedDate = modifiedDate.substring(0, 19);
			}
			modifiedDateDt = new SimpleDateFormat(DATE_FORMAT).parse(modifiedDate);
			lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdate);
		} catch (Exception e) {
			Log.warn("[" + orcidId + "] Parsing date: ", e.getMessage());
			return true;
		}
		return modifiedDateDt.after(lastUpdateDt);
	}
}
