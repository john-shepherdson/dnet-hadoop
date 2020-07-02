
package eu.dnetlib.doiboost.orcid;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

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

	public String testDownloadRecord(String orcidId) throws Exception {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", "Bearer 78fdb232-7105-4086-8570-e153f4198e3d");
			CloseableHttpResponse response = client.execute(httpGet);
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

//	@Test
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

//	@Test
	public void getRecordDatestamp() throws ParseException {
		Date toRetrieveDateDt = new SimpleDateFormat(DATE_FORMAT).parse(toRetrieveDate);
		Date toNotRetrieveDateDt = new SimpleDateFormat(DATE_FORMAT).parse(toNotRetrieveDate);
		Date lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdate);
		assertTrue(toRetrieveDateDt.after(lastUpdateDt));
		assertTrue(!toNotRetrieveDateDt.after(lastUpdateDt));
	}

	public void testDate(String value) throws ParseException {
		System.out.println(value.toString());
		if (value.length() != 19) {
			value = value.substring(0, 19);
		}
		Date valueDt = new SimpleDateFormat(DATE_FORMAT).parse(value);
		System.out.println(valueDt.toString());
	}

//	@Test
	public void testModifiedDate() throws ParseException {
		testDate(toRetrieveDate);
		testDate(toNotRetrieveDate);
		testDate(shortDate);
	}

//	@Test
	public void testReadBase64CompressedRecord() throws Exception {
		final String base64CompressedRecord = IOUtils
			.toString(getClass().getResourceAsStream("0000-0001-6645-509X.compressed.base64"));
		final String recordFromSeqFile = ArgumentApplicationParser.decompressValue(base64CompressedRecord);
		System.out.println(recordFromSeqFile);
		final String downloadedRecord = testDownloadRecord("0000-0001-6645-509X");
		assertTrue(recordFromSeqFile.equals(downloadedRecord));
	}
}
