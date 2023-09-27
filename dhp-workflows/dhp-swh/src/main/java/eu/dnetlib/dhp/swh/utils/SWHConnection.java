
package eu.dnetlib.dhp.swh.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class SWHConnection {

	private static final Logger log = LoggerFactory.getLogger(SWHConnection.class);

	CloseableHttpClient httpClient;

	HttpClientParams clientParams;

	HttpConnector2 conn;

	public SWHConnection(HttpClientParams clientParams) {

//		// force http client to NOT transform double quotes (//) to single quote (/)
//		RequestConfig requestConfig = RequestConfig.custom().setNormalizeUri(false).build();
//
//		// Create an HttpClient instance
//		httpClient = HttpClientBuilder
//			.create()
//			.setDefaultRequestConfig(requestConfig)
//			.build();
//
//		this.clientParams = clientParams;
		// set custom headers
		Map<String, String> headers = new HashMap<String, String>() {
			{
				put(HttpHeaders.ACCEPT, "application/json");
				put(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", SWHConstants.ACCESS_TOKEN));
			}
		};

		clientParams.setHeaders(headers);

		// create http connector
		conn = new HttpConnector2(clientParams);

	}

	public String call(String url) throws CollectorException {
		return conn.getInputSource(url);
	}

	public String getLib(String url) throws IOException, CollectorException {

		// delay between requests
		if (this.clientParams.getRequestDelay() > 0) {
			log.info("Request delay: {}", this.clientParams.getRequestDelay());
			this.backOff(this.clientParams.getRequestDelay());
		}

		// Create an HttpGet request with the URL
		HttpGet httpGet = new HttpGet(url);
		httpGet.setHeader("Accept", "application/json");
		httpGet.setHeader("Authorization", String.format("Bearer %s", SWHConstants.ACCESS_TOKEN));

		// Execute the request and get the response
		try (CloseableHttpResponse response = httpClient.execute(httpGet)) {

			System.out.println(url);

			int responseCode = response.getStatusLine().getStatusCode();
			if (responseCode != HttpStatus.SC_OK) {

			}

			System.out.println(responseCode);

			List<Header> httpHeaders = Arrays.asList(response.getAllHeaders());
			for (Header header : httpHeaders) {
				System.out.println(header.getName() + ":\t" + header.getValue());
			}

			String rateRemaining = this.getRateRemaining(response);

			// back off when rate remaining limit is approaching
			if (rateRemaining != null && (Integer.parseInt(rateRemaining) < 2)) {
				int retryAfter = this.getRetryAfter(response);

				log.info("Rate Limit: {} - Backing off: {}", rateRemaining, retryAfter);
				this.backOff(retryAfter);
			}

			return EntityUtils.toString(response.getEntity());
		}
	}

	private String getRateRemaining(CloseableHttpResponse response) {
		Header header = response.getFirstHeader(Constants.HTTPHEADER_IETF_DRAFT_RATELIMIT_REMAINING);
		if (header != null) {
			return header.getValue();
		}
		return null;
	}

	private int getRetryAfter(CloseableHttpResponse response) {
		Header header = response.getFirstHeader(HttpHeaders.RETRY_AFTER);
		if (header != null) {
			String retryAfter = header.getValue();
			if (NumberUtils.isCreatable(retryAfter)) {
				return Integer.parseInt(retryAfter) + 10;
			}
		}
		return 1000;
	}

	private void backOff(int sleepTimeMs) throws CollectorException {
		try {
			Thread.sleep(sleepTimeMs);
		} catch (InterruptedException e) {
			throw new CollectorException(e);
		}
	}

}
