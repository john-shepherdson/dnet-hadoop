
package eu.dnetlib.doiboost.orcid;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class OrcidClientTest {
	final String orcidId = "0000-0001-7291-3210";
	final int REQ_LIMIT = 24;
	final int REQ_MAX_TEST = 100;
	final int RECORD_DOWNLOADED_COUNTER_LOG_INTERVAL = 10;
	final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	final String toRetrieveDate = "2020-05-06 23:59:46.031145";
	String toNotRetrieveDate = "2019-09-29 23:59:59.000000";
	String lastUpdate = "2019-09-30 00:00:00";
	String shortDate = "2020-05-06 16:06:11";

//	curl -i -H "Accept: application/vnd.orcid+xml"
//	-H 'Authorization: Bearer 78fdb232-7105-4086-8570-e153f4198e3d'
//	'https://api.orcid.org/v3.0/0000-0001-7291-3210/record'

	@Test
	private void multipleDownloadTest() throws Exception {
		int toDownload = 1;
		long start = System.currentTimeMillis();
		OrcidDownloader downloader = new OrcidDownloader();
		TarArchiveInputStream input = new TarArchiveInputStream(
			new GzipCompressorInputStream(new FileInputStream("/tmp/last_modified.csv.tar")));
		TarArchiveEntry entry = input.getNextTarEntry();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		int rowNum = 0;
		int entryNum = 0;
		int modified = 0;
		while (entry != null) {
			br = new BufferedReader(new InputStreamReader(input)); // Read directly from tarInput
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.toString().split(",");
				List<String> recordInfo = Arrays.asList(values);
				String orcidId = recordInfo.get(0);
				if (downloader.isModified(orcidId, recordInfo.get(3))) {
					downloadTest(orcidId);
					modified++;
				}
				rowNum++;
				if (modified > toDownload) {
					break;
				}
			}
			entryNum++;
			entry = input.getNextTarEntry();
		}
		long end = System.currentTimeMillis();
		logToFile("start test: " + new Date(start).toString());
		logToFile("end test: " + new Date(end).toString());
	}

	@Test
	private void downloadTest(String orcid) throws Exception {
		String record = testDownloadRecord(orcid);
		String filename = "/tmp/downloaded_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	private String testDownloadRecord(String orcidId) throws Exception {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", "Bearer 78fdb232-7105-4086-8570-e153f4198e3d");
			logToFile("start connection: " + new Date(System.currentTimeMillis()).toString());
			CloseableHttpResponse response = client.execute(httpGet);
			logToFile("end connection: " + new Date(System.currentTimeMillis()).toString());
			if (response.getStatusLine().getStatusCode() != 200) {
				System.out
					.println("Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
			}
			return IOUtils.toString(response.getEntity().getContent());
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return new String("");
	}

	// @Test
	private void testLambdaFileParser() throws Exception {
		try (BufferedReader br = new BufferedReader(
			new InputStreamReader(this.getClass().getResourceAsStream("last_modified.csv")))) {
			String line;
			int counter = 0;
			int nReqTmp = 0;
			long startDownload = System.currentTimeMillis();
			long startReqTmp = System.currentTimeMillis();
			while ((line = br.readLine()) != null) {
				counter++;
//		    	skip headers line
				if (counter == 1) {
					continue;
				}
				String[] values = line.split(",");
				List<String> recordInfo = Arrays.asList(values);
				testDownloadRecord(recordInfo.get(0));
				long endReq = System.currentTimeMillis();
				nReqTmp++;
				if (nReqTmp == REQ_LIMIT) {
					long reqSessionDuration = endReq - startReqTmp;
					if (reqSessionDuration <= 1000) {
						System.out
							.println(
								"\nreqSessionDuration: " + reqSessionDuration + " nReqTmp: " + nReqTmp + " wait ....");
						Thread.sleep(1000 - reqSessionDuration);
					} else {
						nReqTmp = 0;
						startReqTmp = System.currentTimeMillis();
					}
				}

				if (counter > REQ_MAX_TEST) {
					break;
				}
				if ((counter % RECORD_DOWNLOADED_COUNTER_LOG_INTERVAL) == 0) {
					System.out.println("Current record downloaded: " + counter);
				}
			}
			long endDownload = System.currentTimeMillis();
			long downloadTime = endDownload - startDownload;
			System.out.println("Download time: " + ((downloadTime / 1000) / 60) + " minutes");
		}
	}

	// @Test
	private void getRecordDatestamp() throws ParseException {
		Date toRetrieveDateDt = new SimpleDateFormat(DATE_FORMAT).parse(toRetrieveDate);
		Date toNotRetrieveDateDt = new SimpleDateFormat(DATE_FORMAT).parse(toNotRetrieveDate);
		Date lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdate);
		assertTrue(toRetrieveDateDt.after(lastUpdateDt));
		assertTrue(!toNotRetrieveDateDt.after(lastUpdateDt));
	}

	private void testDate(String value) throws ParseException {
		System.out.println(value.toString());
		if (value.length() != 19) {
			value = value.substring(0, 19);
		}
		Date valueDt = new SimpleDateFormat(DATE_FORMAT).parse(value);
		System.out.println(valueDt.toString());
	}

	// @Test
	@Ignore
	private void testModifiedDate() throws ParseException {
		testDate(toRetrieveDate);
		testDate(toNotRetrieveDate);
		testDate(shortDate);
	}

	@Test
	public void testReadBase64CompressedRecord() throws Exception {
		final String base64CompressedRecord = IOUtils
			.toString(getClass().getResourceAsStream("0000-0003-3028-6161.compressed.base64"));
		final String recordFromSeqFile = ArgumentApplicationParser.decompressValue(base64CompressedRecord);
		logToFile("\n\ndownloaded \n\n" + recordFromSeqFile);
		final String downloadedRecord = testDownloadRecord("0000-0003-3028-6161");
		assertTrue(recordFromSeqFile.equals(downloadedRecord));
	}

	@Test
	private void lambdaFileReaderTest() throws Exception {
		TarArchiveInputStream input = new TarArchiveInputStream(
			new GzipCompressorInputStream(new FileInputStream("/develop/last_modified.csv.tar")));
		TarArchiveEntry entry = input.getNextTarEntry();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		int rowNum = 0;
		int entryNum = 0;
		while (entry != null) {
			br = new BufferedReader(new InputStreamReader(input)); // Read directly from tarInput
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.toString().split(",");
				List<String> recordInfo = Arrays.asList(values);
				assertTrue(recordInfo.size() == 4);

				rowNum++;
				if (rowNum == 1) {
					assertTrue(recordInfo.get(3).equals("last_modified"));
				} else if (rowNum == 2) {
					assertTrue(recordInfo.get(0).equals("0000-0002-0499-7333"));
				}
			}
			entryNum++;
			assertTrue(entryNum == 1);
			entry = input.getNextTarEntry();
		}
	}

	@Test
	private void lambdaFileCounterTest() throws Exception {
		final String lastUpdate = "2020-09-29 00:00:00";
		OrcidDownloader downloader = new OrcidDownloader();
		TarArchiveInputStream input = new TarArchiveInputStream(
			new GzipCompressorInputStream(new FileInputStream("/tmp/last_modified.csv.tar")));
		TarArchiveEntry entry = input.getNextTarEntry();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		int rowNum = 0;
		int entryNum = 0;
		int modified = 0;
		while (entry != null) {
			br = new BufferedReader(new InputStreamReader(input)); // Read directly from tarInput
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.toString().split(",");
				List<String> recordInfo = Arrays.asList(values);
				String orcidId = recordInfo.get(0);
				if (downloader.isModified(orcidId, recordInfo.get(3))) {
					modified++;
				}
				rowNum++;
			}
			entryNum++;
			entry = input.getNextTarEntry();
		}
		logToFile("rowNum: " + rowNum);
		logToFile("modified: " + modified);
	}

	private void logToFile(String log)
		throws IOException {
		log = log.concat("\n");
		Path path = Paths.get("/tmp/orcid_log.txt");
		Files.write(path, log.getBytes(), StandardOpenOption.APPEND);
	}
}
