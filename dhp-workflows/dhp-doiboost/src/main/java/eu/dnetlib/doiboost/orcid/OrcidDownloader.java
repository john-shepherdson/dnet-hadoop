
package eu.dnetlib.doiboost.orcid;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class OrcidDownloader extends OrcidDSManager {

	static final int REQ_LIMIT = 24;
	static final int REQ_MAX_TEST = -1;
	static final int RECORD_PARSED_COUNTER_LOG_INTERVAL = 500;
	static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	static final String lastUpdate = "2020-09-29 00:00:00";
	private String lambdaFileName;
	private String outputPath;
	private String token;

	public static void main(String[] args) throws IOException, Exception {
		OrcidDownloader orcidDownloader = new OrcidDownloader();
		orcidDownloader.loadArgs(args);
		orcidDownloader.parseLambdaFile();
	}

	private String downloadRecord(String orcidId) throws IOException {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", String.format("Bearer %s", token));
			CloseableHttpResponse response = client.execute(httpGet);
			if (response.getStatusLine().getStatusCode() != 200) {
				Log
					.info(
						"Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
				return new String("");
			}
//			return IOUtils.toString(response.getEntity().getContent());
			return xmlStreamToString(response.getEntity().getContent());
		}
	}

	private String xmlStreamToString(InputStream xmlStream) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(xmlStream));
		String line;
		StringBuffer buffer = new StringBuffer();
		while ((line = br.readLine()) != null) {
			buffer.append(line);
		}
		return buffer.toString();
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
				.concat("updated_xml_authors.seq"));
		try (TarArchiveInputStream tais = new TarArchiveInputStream(
			new GzipCompressorInputStream(lambdaFileStream))) {
			TarArchiveEntry entry = null;
			StringBuilder sb = new StringBuilder();
			try (SequenceFile.Writer writer = SequenceFile
				.createWriter(
					conf,
					SequenceFile.Writer.file(hdfsoutputPath),
					SequenceFile.Writer.keyClass(Text.class),
					SequenceFile.Writer.valueClass(Text.class),
					SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new GzipCodec()))) {
				startDownload = System.currentTimeMillis();
				while ((entry = tais.getNextTarEntry()) != null) {
					BufferedReader br = new BufferedReader(new InputStreamReader(tais)); // Read directly from tarInput
					String line;
					while ((line = br.readLine()) != null) {
						String[] values = line.split(",");
						List<String> recordInfo = Arrays.asList(values);
						int nReqTmp = 0;
						long startReqTmp = System.currentTimeMillis();
						// skip headers line
						if (parsedRecordsCounter == 0) {
							parsedRecordsCounter++;
							continue;
						}
						parsedRecordsCounter++;
						String orcidId = recordInfo.get(0);
						if (isModified(orcidId, recordInfo.get(3))) {
							String record = downloadRecord(orcidId);
							downloadedRecordsCounter++;
							if (!record.isEmpty()) {
//							String compressRecord = ArgumentApplicationParser.compressArgument(record);
								final Text key = new Text(recordInfo.get(0));
								final Text value = new Text(record);
								writer.append(key, value);
								savedRecordsCounter++;
							}
						} else {
							break;
						}
						long endReq = System.currentTimeMillis();
						nReqTmp++;
						if (nReqTmp == REQ_LIMIT) {
							long reqSessionDuration = endReq - startReqTmp;
							if (reqSessionDuration <= 1000) {
								Log
									.info(
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
						if ((parsedRecordsCounter % RECORD_PARSED_COUNTER_LOG_INTERVAL) == 0) {
							Log
								.info(
									"Current parsed: "
										+ parsedRecordsCounter
										+ " downloaded: "
										+ downloadedRecordsCounter
										+ " saved: "
										+ savedRecordsCounter);
							if (REQ_MAX_TEST != -1 && parsedRecordsCounter > REQ_MAX_TEST) {
								break;
							}
						}
					}
					long endDownload = System.currentTimeMillis();
					long downloadTime = endDownload - startDownload;
					Log.info("Download time: " + ((downloadTime / 1000) / 60) + " minutes");
				}
			}
		}
		Log.info("Download started at: " + new Date(startDownload).toString());
		Log.info("Download ended at: " + new Date(System.currentTimeMillis()).toString());
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

	public boolean isModified(String orcidId, String modifiedDate) {
		Date modifiedDateDt = null;
		Date lastUpdateDt = null;
		try {
			if (modifiedDate.length() != 19) {
				modifiedDate = modifiedDate.substring(0, 19);
			}
			modifiedDateDt = new SimpleDateFormat(DATE_FORMAT).parse(modifiedDate);
			lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdate);
		} catch (Exception e) {
			Log.info("[" + orcidId + "] Parsing date: ", e.getMessage());
			return true;
		}
		return modifiedDateDt.after(lastUpdateDt);
	}
}
