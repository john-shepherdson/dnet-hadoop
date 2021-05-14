
package eu.dnetlib.doiboost.orcid;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParserTest;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class OrcidClientTest {
	final int REQ_LIMIT = 24;
	final int REQ_MAX_TEST = 100;
	final int RECORD_DOWNLOADED_COUNTER_LOG_INTERVAL = 10;
	final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	final String toRetrieveDate = "2020-05-06 23:59:46.031145";
	String toNotRetrieveDate = "2019-09-29 23:59:59.000000";
	String lastUpdate = "2019-09-30 00:00:00";
	String shortDate = "2020-05-06 16:06:11";
	final String REQUEST_TYPE_RECORD = "record";
	final String REQUEST_TYPE_WORK = "work/47652866";
	final String REQUEST_TYPE_WORKS = "works";

	private static Path testPath;

	@BeforeAll
	private static void setUp() throws IOException {
		testPath = Files.createTempDirectory(XMLRecordParserTest.class.getName());
		System.out.println("using test path: " + testPath);
	}

//	curl -i -H "Accept: application/vnd.orcid+xml"
//	-H 'Authorization: Bearer 78fdb232-7105-4086-8570-e153f4198e3d'
//	'https://api.orcid.org/v3.0/0000-0001-7291-3210/record'

	@Test
	public void downloadTest() throws Exception {
		final String orcid = "0000-0001-7291-3210";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_RECORD);
		String filename = testPath + "/downloaded_record_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	private String testDownloadRecord(String orcidId, String dataType) throws Exception {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/" + dataType);
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", "Bearer 78fdb232-7105-4086-8570-e153f4198e3d");
			long start = System.currentTimeMillis();
			CloseableHttpResponse response = client.execute(httpGet);
			long end = System.currentTimeMillis();
			if (response.getStatusLine().getStatusCode() != 200) {
				logToFile(
					testPath, "Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
			}
			logToFile(testPath, orcidId + " " + dataType + " " + (end - start) / 1000 + " seconds");
			return IOUtils.toString(response.getEntity().getContent());
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return "";
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
				testDownloadRecord(recordInfo.get(0), REQUEST_TYPE_RECORD);
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
		System.out.println(value);
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
	private void testReadBase64CompressedRecord() throws Exception {
		final String base64CompressedRecord = IOUtils
			.toString(getClass().getResourceAsStream("0000-0003-3028-6161.compressed.base64"));
		final String recordFromSeqFile = ArgumentApplicationParser.decompressValue(base64CompressedRecord);
		logToFile(testPath, "\n\ndownloaded \n\n" + recordFromSeqFile);
		final String downloadedRecord = testDownloadRecord("0000-0003-3028-6161", REQUEST_TYPE_RECORD);
		assertTrue(recordFromSeqFile.equals(downloadedRecord));
	}

	@Test
	@Disabled
	public void lambdaFileReaderTest() throws Exception {
		String last_update = "2021-01-12 00:00:06.685137";
		TarArchiveInputStream input = new TarArchiveInputStream(
			new GzipCompressorInputStream(new FileInputStream("/tmp/last_modified.csv.tar")));
		TarArchiveEntry entry = input.getNextTarEntry();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		int rowNum = 1;
		int modifiedNum = 1;
		int entryNum = 0;
		boolean firstNotModifiedFound = false;
		while (entry != null) {
			br = new BufferedReader(new InputStreamReader(input)); // Read directly from tarInput
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				List<String> recordInfo = Arrays.asList(values);
				assertTrue(recordInfo.size() == 4);
				String orcid = recordInfo.get(0);
				String modifiedDate = recordInfo.get(3);
				rowNum++;
				if (rowNum == 2) {
					assertTrue(recordInfo.get(3).equals("last_modified"));
				} else {
//					SparkDownloadOrcidAuthors.lastUpdate = last_update;
//					boolean isModified = SparkDownloadOrcidAuthors.isModified(orcid, modifiedDate);
//					if (isModified) {
//						modifiedNum++;
//					} else {
//						if (!firstNotModifiedFound) {
//							firstNotModifiedFound = true;
//							logToFile(orcid + " - " + modifiedDate + " > " + isModified);
//						}
//					}

				}
			}
			entryNum++;
			assertTrue(entryNum == 1);
			entry = input.getNextTarEntry();

		}
		logToFile(testPath, "modifiedNum : " + modifiedNum + " / " + rowNum);
	}

	public static void logToFile(Path basePath, String log) throws IOException {
		log = log.concat("\n");
		Path path = basePath.resolve("orcid_log.txt");
		if (!Files.exists(path)) {
			Files.createFile(path);
		}
		Files.write(path, log.getBytes(), StandardOpenOption.APPEND);
	}

	@Test
	@Disabled
	private void slowedDownDownloadTest() throws Exception {
		String orcid = "0000-0001-5496-1243";
		String record = slowedDownDownload(orcid);
		String filename = "/tmp/downloaded_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	private String slowedDownDownload(String orcidId) throws Exception {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", "Bearer 78fdb232-7105-4086-8570-e153f4198e3d");
			long start = System.currentTimeMillis();
			CloseableHttpResponse response = client.execute(httpGet);
			long endReq = System.currentTimeMillis();
			long reqSessionDuration = endReq - start;
			logToFile(testPath, "req time (millisec): " + reqSessionDuration);
			if (reqSessionDuration < 1000) {
				logToFile(testPath, "wait ....");
				Thread.sleep(1000 - reqSessionDuration);
			}
			long end = System.currentTimeMillis();
			long total = end - start;
			logToFile(testPath, "total time (millisec): " + total);
			if (response.getStatusLine().getStatusCode() != 200) {
				logToFile(
					testPath, "Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
			}
			return IOUtils.toString(response.getEntity().getContent());
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return "";
	}

	@Test
	public void downloadWorkTest() throws Exception {
		String orcid = "0000-0003-0015-1952";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_WORK);
		String filename = "/tmp/downloaded_work_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	public void downloadRecordTest() throws Exception {
		String orcid = "0000-0001-5004-5918";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_RECORD);
		String filename = "/tmp/downloaded_record_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	public void downloadWorksTest() throws Exception {
		String orcid = "0000-0001-5004-5918";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_WORKS);
		String filename = "/tmp/downloaded_works_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	public void downloadSingleWorkTest() throws Exception {
		String orcid = "0000-0001-5004-5918";
		String record = testDownloadRecord(orcid, REQUEST_TYPE_WORK);
		String filename = "/tmp/downloaded_work_47652866_".concat(orcid).concat(".xml");
		File f = new File(filename);
		OutputStream outStream = new FileOutputStream(f);
		IOUtils.write(record.getBytes(), outStream);
	}

	@Test
	public void cleanAuthorListTest() throws Exception {
		AuthorData a1 = new AuthorData();
		a1.setOid("1");
		a1.setName("n1");
		a1.setSurname("s1");
		a1.setCreditName("c1");
		AuthorData a2 = new AuthorData();
		a2.setOid("1");
		a2.setName("n1");
		a2.setSurname("s1");
		a2.setCreditName("c1");
		AuthorData a3 = new AuthorData();
		a3.setOid("3");
		a3.setName("n3");
		a3.setSurname("s3");
		a3.setCreditName("c3");
		List<AuthorData> list = Lists.newArrayList();
		list.add(a1);
		list.add(a2);
		list.add(a3);

		Set<String> namesAlreadySeen = new HashSet<>();
		assertTrue(list.size() == 3);
		list.removeIf(a -> !namesAlreadySeen.add(a.getOid()));
		assertTrue(list.size() == 2);
	}

	@Test
	@Ignore
	public void testUpdatedRecord() throws Exception {
		final String base64CompressedRecord = IOUtils
			.toString(getClass().getResourceAsStream("0000-0003-3028-6161.compressed.base64"));
		final String record = ArgumentApplicationParser.decompressValue(base64CompressedRecord);
		logToFile(testPath, "\n\nrecord updated \n\n" + record);
	}

	@Test
	@Ignore
	private void testUpdatedWork() throws Exception {
		final String base64CompressedWork = "H4sIAAAAAAAAAM1XS2/jNhC+51cQOuxJsiXZSR03Vmq0G6Bo013E6R56oyXaZiOJWpKy4y783zvUg5Ksh5uiCJogisX5Zjj85sHx3f1rFKI94YKyeGE4I9tAJPZZQOPtwvj9+cGaGUhIHAc4ZDFZGEcijHvv6u7A+MtcPVCSSgsUQObYzuzaccBEguVuYYxt+LHgbwKP6a11M3WnY6UzrpB7KuiahlQeF0aSrkPqGwhcisWcxpLwGIcLYydlMh+PD4fDiHGfBvDcjmMxLhGlBglSH8vsIH0qGlLqBFRIGvvDWjWQ1iMJJ2CKBANqGlNqMbkj3IpxRPq1KkypFZFoDRHa0aRfq8JoNjhnfIAJJS6xPouiIQJyeYmGQzE+cO5cXqITcItBlKyASExD0a93jiwtvJDjYXDDAqBPHoH2wMmVWGNf8xyyaEBiSTeUDHHWBpd2Nmmc10yfbgHQrHCyIRxKjQwRUoFKPRwEnIgBnQJQVdGeQgJaCRN0OMnPkaUFVbD9WkpaIndQJowf+8EFoIpTErJjBFQOBavElFpfUxwC9ZcqvQErdQXhe+oPFF8BaObupYzVsYEOARzSoZBWmKqaBMHcV0Wf8oG0beIqD+Gdkz0lhyE3NajUW6fhQFSV9Nw/MCBYyofYa0EN7wrBz13eP+Y+J6obWgE8Pdd2JpYD94P77Ezmjj13b0bu5PqPu3EXumEnxEJaEVxSUIHammsra+53z44zt2/m1/bItaeVtQ6dhs3c4XytvW75IYUchMKvEHVUyqmnWBFAS0VJrqSvQde6vp251ux2NtFuKcVOi+oK9YY0M0Cn6o4J6WkvtEK2XJ1vfPGAZxSoK8lb+SxJBbLQx1CohOLndjJUywQWUFmqEi3G6Zaqf/7buOyYJd5IYpfmf0XipfP18pDR9cQCeEuJQI/Lx36bFbVnpBeL2UwmqQw7ApAvf4GeGGQdEbENgolui/wdpjHaYCmPCIPPAmGBIsxfoLUhyRCB0SeCakEBJRKBtfJ+UBbI15TG4PaGBAhWthx8DmFYtHZQujv1CWbLLdzmmUKmHEOWCe1/zdu78bn/+YH+hCOqOzcXfFwuP6OVT/P710crwqGXFrpNaM2GT3MXarw01i15TIi3pmtJXgtbTVGf3h6HKfF+wBAnPyTfdCChudlm5gZaoG//F9pPZsGQcqqbyZN5hBau5OoIJ3PPwjTKDuG4s5MZp2rMzF5PZoK34IT6PIFOPrk+mTiVO5aJH2C+JJRjE/06eoRfpJxa4VgyYaLlaJUv/EhCfATMU/76gEOfmehL/qbJNNHjaFna+CQYB8wvo9PpPFJ5MOrJ1Ix7USBZqBl7KRNOx1d3jex7SG6zuijqCMWRusBsncjZSrM2u82UJmqzpGhvUJN2t6caIM9QQgO9c0t40UROnWsJd2Rbs+nsxpna9u30ttNkjechmzHjEST+X5CkkuNY0GzQkzyFseAf7lSZuLwdh1xSXKvvQJ4g4abTYgPV7uMt3rskohlJmMa82kQkshtyBEIYqQ+YB8X3oRHg7iFKi/bZP+Ao+T6BJhIT/vNPi8ffZs+flk+r2v0WNroZiyWn6xRmadHqTJXsjLJczElAZX6TnJdoWTM1SI2gfutv3rjeBt5t06rVvNuWup29246tlvluO+u2/G92bK9DXheL6uFd/Q3EaRDZqBIAAA==";
		final String work = ArgumentApplicationParser.decompressValue(base64CompressedWork);
		logToFile(testPath, "\n\nwork updated \n\n" + work);
	}
}
