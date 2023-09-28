
package eu.dnetlib.dhp.swh;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.swh.utils.SWHConnection;
import eu.dnetlib.dhp.swh.utils.SWHConstants;

//import org.apache.hadoop.hdfs.MiniDFSCluster;

public class SWHConnectionTest {
	private static final Logger log = LoggerFactory.getLogger(SWHConnectionTest.class);

	@Test
	void testGetCall() throws IOException {

		HttpClientParams clientParams = new HttpClientParams();
		clientParams.setRequestMethod("GET");

		SWHConnection swhConnection = new SWHConnection(clientParams);

		String repoUrl = "https://github.com/stanford-futuredata/FAST";
		URL url = new URL(String.format(SWHConstants.SWH_LATEST_VISIT_URL, repoUrl));
		String response = null;
		try {
			response = swhConnection.call(url.toString());
		} catch (CollectorException e) {
			System.out.println("Error in request: " + url);
		}
		System.out.println(response);
	}

	@Test
	void testPostCall() throws MalformedURLException {
		HttpClientParams clientParams = new HttpClientParams();
		clientParams.setRequestMethod("POST");

		SWHConnection swhConnection = new SWHConnection(clientParams);

		String repoUrl = "https://github.com/stanford-futuredata/FAST";
		URL url = new URL(String.format(SWHConstants.SWH_ARCHIVE_URL, SWHConstants.DEFAULT_VISIT_TYPE, repoUrl));
		String response = null;
		try {
			response = swhConnection.call(url.toString());
		} catch (CollectorException e) {
			System.out.println("Error in request: " + url);
		}
		System.out.println(response);
	}
}
