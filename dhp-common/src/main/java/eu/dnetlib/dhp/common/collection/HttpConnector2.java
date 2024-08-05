
package eu.dnetlib.dhp.common.collection;

import static eu.dnetlib.dhp.utils.DHPUtils.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

/**
 * Migrated from https://svn.driver.research-infrastructures.eu/driver/dnet45/modules/dnet-modular-collector-service/trunk/src/main/java/eu/dnetlib/data/collector/plugins/HttpConnector.java
 *
 * @author jochen, michele, andrea, alessia, claudio, andreas
 */
public class HttpConnector2 {

	private static final Logger log = LoggerFactory.getLogger(HttpConnector2.class);

	private static final String REPORT_PREFIX = "http:";

	private HttpClientParams clientParams;

	private String responseType = null;

	private static final String userAgent = "Mozilla/5.0 (compatible; OAI; +http://www.openaire.eu)";

	public HttpConnector2() {
		this(new HttpClientParams());
	}

	public HttpConnector2(HttpClientParams clientParams) {
		this.clientParams = clientParams;
		CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
	}

	/**
	 * @see HttpConnector2#getInputSource(java.lang.String, AggregatorReport)
	 */
	public InputStream getInputSourceAsStream(final String requestUrl) throws CollectorException {
		return IOUtils.toInputStream(getInputSource(requestUrl));
	}

	/**
	 * @see HttpConnector2#getInputSource(java.lang.String, AggregatorReport)
	 */
	public String getInputSource(final String requestUrl) throws CollectorException {
		return attemptDownloadAsString(requestUrl, 1, new AggregatorReport());
	}

	/**
	 * Given the URL returns the content via HTTP GET
	 *
	 * @param requestUrl the URL
	 * @param report the list of errors
	 * @return the content of the downloaded resource
	 * @throws CollectorException when retrying more than maxNumberOfRetry times
	 */
	public String getInputSource(final String requestUrl, AggregatorReport report)
		throws CollectorException {
		return attemptDownloadAsString(requestUrl, 1, report);
	}

	private String attemptDownloadAsString(final String requestUrl, final int retryNumber,
		final AggregatorReport report) throws CollectorException {

		try (InputStream s = attemptDownload(requestUrl, retryNumber, report)) {
			return IOUtils.toString(s);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			throw new CollectorException(e);
		}
	}

	private InputStream attemptDownload(final String requestUrl, final int retryNumber,
		final AggregatorReport report) throws CollectorException, IOException {

		if (retryNumber > getClientParams().getMaxNumberOfRetry()) {
			final String msg = String
				.format(
					"Max number of retries (%s/%s) exceeded, failing.",
					retryNumber, getClientParams().getMaxNumberOfRetry());
			log.error(msg);
			throw new CollectorException(msg);
		}

		InputStream input = null;

		long start = System.currentTimeMillis();
		try {
			if (getClientParams().getRequestDelay() > 0) {
				backoffAndSleep(getClientParams().getRequestDelay());
			}

			log.info("Request attempt {} [{}]", retryNumber, requestUrl);

			final HttpURLConnection urlConn = (HttpURLConnection) new URL(requestUrl).openConnection();
			urlConn.setInstanceFollowRedirects(false);
			urlConn.setReadTimeout(getClientParams().getReadTimeOut() * 1000);
			urlConn.setConnectTimeout(getClientParams().getConnectTimeOut() * 1000);
			urlConn.addRequestProperty(HttpHeaders.USER_AGENT, userAgent);
			urlConn.setRequestMethod(getClientParams().getRequestMethod());

			// if provided, add custom headers
			if (!getClientParams().getHeaders().isEmpty()) {
				for (Map.Entry<String, String> headerEntry : getClientParams().getHeaders().entrySet()) {
					urlConn.addRequestProperty(headerEntry.getKey(), headerEntry.getValue());
				}
			}

			logHeaderFields(urlConn);

			int retryAfter = obtainRetryAfter(urlConn.getHeaderFields());
			String rateLimit = urlConn.getHeaderField(Constants.HTTPHEADER_IETF_DRAFT_RATELIMIT_LIMIT);
			String rateRemaining = urlConn.getHeaderField(Constants.HTTPHEADER_IETF_DRAFT_RATELIMIT_REMAINING);

			if ((rateLimit != null) && (rateRemaining != null) && (Integer.parseInt(rateRemaining) < 2)) {
				if (retryAfter > 0) {
					backoffAndSleep(retryAfter);
				} else {
					backoffAndSleep(1000);
				}
			}

			if (is2xx(urlConn.getResponseCode())) {
				return getInputStream(urlConn, start);
			}
			if (is3xx(urlConn.getResponseCode())) {
				// REDIRECTS
				final String newUrl = obtainNewLocation(urlConn.getHeaderFields());
				log.info("The requested url has been moved to {}", newUrl);
				report
					.put(
						REPORT_PREFIX + urlConn.getResponseCode(),
						String.format("Moved to: %s", newUrl));
				logRequestTime(start);
				urlConn.disconnect();
				if (retryAfter > 0) {
					backoffAndSleep(retryAfter);
				}
				return attemptDownload(newUrl, retryNumber + 1, report);
			}
			if (is4xx(urlConn.getResponseCode()) || is5xx(urlConn.getResponseCode())) {
				switch (urlConn.getResponseCode()) {
					case HttpURLConnection.HTTP_NOT_FOUND:
					case HttpURLConnection.HTTP_BAD_GATEWAY:
					case HttpURLConnection.HTTP_UNAVAILABLE:
					case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
						if (retryAfter > 0) {
							log
								.warn(
									"waiting and repeating request after suggested retry-after {} sec for URL {}",
									retryAfter, requestUrl);
							backoffAndSleep(retryAfter * 1000);
						} else {
							log
								.warn(
									"waiting and repeating request after default delay of {} sec for URL {}",
									getClientParams().getRetryDelay(), requestUrl);
							backoffAndSleep(retryNumber * getClientParams().getRetryDelay());
						}
						report.put(REPORT_PREFIX + urlConn.getResponseCode(), requestUrl);

						logRequestTime(start);

						urlConn.disconnect();

						return attemptDownload(requestUrl, retryNumber + 1, report);
					case 422: // UNPROCESSABLE ENTITY
						report.put(REPORT_PREFIX + urlConn.getResponseCode(), requestUrl);
						log.warn("waiting and repeating request after 10 sec for URL {}", requestUrl);
						backoffAndSleep(10000);
						urlConn.disconnect();
						logRequestTime(start);
						try {
							return getInputStream(urlConn, start);
						} catch (IOException e) {
							log
								.error(
									"server returned 422 and got IOException accessing the response body from URL {}",
									requestUrl);
							log.error("IOException:", e);
							return attemptDownload(requestUrl, retryNumber + 1, report);
						}
					default:
						log.error("gor error {} from URL: {}", urlConn.getResponseCode(), urlConn.getURL());
						log.error("response message: {}", urlConn.getResponseMessage());
						report
							.put(
								REPORT_PREFIX + urlConn.getResponseCode(),
								String
									.format(
										"%s Error: %s", requestUrl, urlConn.getResponseMessage()));
						logRequestTime(start);
						urlConn.disconnect();
						throw new CollectorException(urlConn.getResponseCode() + " error " + report);
				}
			}
			throw new CollectorException(
				String
					.format(
						"Unexpected status code: %s errors: %s", urlConn.getResponseCode(),
						MAPPER.writeValueAsString(report)));
		} catch (MalformedURLException e) {
			log.error(e.getMessage(), e);
			report.put(e.getClass().getName(), e.getMessage());
			throw new CollectorException(e.getMessage(), e);
		} catch (SocketTimeoutException | SocketException | UnknownHostException e) {
			log.error(e.getMessage(), e);
			report.put(e.getClass().getName(), e.getMessage());
			backoffAndSleep(getClientParams().getRetryDelay() * retryNumber * 1000);
			return attemptDownload(requestUrl, retryNumber + 1, report);
		}
	}

	private InputStream getInputStream(HttpURLConnection urlConn, long start) throws IOException {
		InputStream input = urlConn.getInputStream();
		responseType = urlConn.getContentType();
		logRequestTime(start);
		return input;
	}

	private static void logRequestTime(long start) {
		log
			.info(
				"request time elapsed: {}sec",
				TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
	}

	private void logHeaderFields(final HttpURLConnection urlConn) throws IOException {
		log.info("Response: {} - {}", urlConn.getResponseCode(), urlConn.getResponseMessage());

		for (Map.Entry<String, List<String>> e : urlConn.getHeaderFields().entrySet()) {
			if (e.getKey() != null) {
				for (String v : e.getValue()) {
					log.info("  key: {} - value: {}", e.getKey(), v);
				}
			}
		}
	}

	private void backoffAndSleep(int sleepTimeMs) throws CollectorException {
		log.info("I'm going to sleep for {}ms", sleepTimeMs);
		try {
			Thread.sleep(sleepTimeMs);
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
			throw new CollectorException(e);
		}
	}

	private int obtainRetryAfter(final Map<String, List<String>> headerMap) {
		for (String key : headerMap.keySet()) {
			if ((key != null) && key.equalsIgnoreCase(HttpHeaders.RETRY_AFTER) && (!headerMap.get(key).isEmpty())
				&& NumberUtils.isCreatable(headerMap.get(key).get(0))) {
				return Integer.parseInt(headerMap.get(key).get(0));
			}
		}
		return -1;
	}

	private String obtainNewLocation(final Map<String, List<String>> headerMap) throws CollectorException {
		for (String key : headerMap.keySet()) {
			if ((key != null) && key.equalsIgnoreCase(HttpHeaders.LOCATION) && (headerMap.get(key).size() > 0)) {
				return headerMap.get(key).get(0);
			}
		}
		throw new CollectorException("The requested url has been MOVED, but 'location' param is MISSING");
	}

	private boolean is2xx(final int statusCode) {
		return statusCode >= 200 && statusCode <= 299;
	}

	private boolean is4xx(final int statusCode) {
		return statusCode >= 400 && statusCode <= 499;
	}

	private boolean is3xx(final int statusCode) {
		return statusCode >= 300 && statusCode <= 399;
	}

	private boolean is5xx(final int statusCode) {
		return statusCode >= 500 && statusCode <= 599;
	}

	public String getResponseType() {
		return responseType;
	}

	public HttpClientParams getClientParams() {
		return clientParams;
	}

	public void setClientParams(HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}
}
