
package eu.dnetlib.dhp.collection.worker;

import static eu.dnetlib.dhp.utils.DHPUtils.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrated from https://svn.driver.research-infrastructures.eu/driver/dnet45/modules/dnet-modular-collector-service/trunk/src/main/java/eu/dnetlib/data/collector/plugins/HttpConnector.java
 *
 * @author jochen, michele, andrea, alessia, claudio
 */
public class HttpConnector2 {

	private static final Logger log = LoggerFactory.getLogger(HttpConnector2.class);

	private static final String REPORT_PREFIX = "http:";

	private HttpClientParams clientParams;

	private String responseType = null;

	private String userAgent = "Mozilla/5.0 (compatible; OAI; +http://www.openaire.eu)";

	public HttpConnector2() {
		this(new HttpClientParams());
	}

	public HttpConnector2(HttpClientParams clientParams) {
		this.clientParams = clientParams;
		CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
	}

	/**
	 * @see HttpConnector2#getInputSource(java.lang.String, CollectorPluginReport)
	 */
	public InputStream getInputSourceAsStream(final String requestUrl) throws CollectorException {
		return IOUtils.toInputStream(getInputSource(requestUrl));
	}

	/**
	 * @see HttpConnector2#getInputSource(java.lang.String, CollectorPluginReport)
	 */
	public String getInputSource(final String requestUrl) throws CollectorException {
		return attemptDownloadAsString(requestUrl, 1, new CollectorPluginReport());
	}

	/**
	 * Given the URL returns the content via HTTP GET
	 *
	 * @param requestUrl the URL
	 * @param report the list of errors
	 * @return the content of the downloaded resource
	 * @throws CollectorException when retrying more than maxNumberOfRetry times
	 */
	public String getInputSource(final String requestUrl, CollectorPluginReport report)
		throws CollectorException {
		return attemptDownloadAsString(requestUrl, 1, report);
	}

	private String attemptDownloadAsString(final String requestUrl, final int retryNumber,
		final CollectorPluginReport report) throws CollectorException {

		try (InputStream s = attemptDownload(requestUrl, retryNumber, report)) {
			return IOUtils.toString(s);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			throw new CollectorException(e);
		}
	}

	private InputStream attemptDownload(final String requestUrl, final int retryNumber,
		final CollectorPluginReport report) throws CollectorException, IOException {

		if (retryNumber > getClientParams().getMaxNumberOfRetry()) {
			final String msg = String
				.format(
					"Max number of retries (%s/%s) exceeded, failing.",
					retryNumber, getClientParams().getMaxNumberOfRetry());
			log.error(msg);
			throw new CollectorException(msg);
		}

		log.info("Request attempt {} [{}]", retryNumber, requestUrl);

		InputStream input = null;

		try {
			if (getClientParams().getRequestDelay() > 0) {
				backoffAndSleep(getClientParams().getRequestDelay());
			}
			final HttpURLConnection urlConn = (HttpURLConnection) new URL(requestUrl).openConnection();
			urlConn.setInstanceFollowRedirects(false);
			urlConn.setReadTimeout(getClientParams().getReadTimeOut() * 1000);
			urlConn.setConnectTimeout(getClientParams().getConnectTimeOut() * 1000);
			urlConn.addRequestProperty(HttpHeaders.USER_AGENT, userAgent);

			if (log.isDebugEnabled()) {
				logHeaderFields(urlConn);
			}

			int retryAfter = obtainRetryAfter(urlConn.getHeaderFields());
			if (is2xx(urlConn.getResponseCode())) {
				input = urlConn.getInputStream();
				responseType = urlConn.getContentType();
				return input;
			}
			if (is3xx(urlConn.getResponseCode())) {
				// REDIRECTS
				final String newUrl = obtainNewLocation(urlConn.getHeaderFields());
				log.info(String.format("The requested url has been moved to %s", newUrl));
				report
					.put(
						REPORT_PREFIX + urlConn.getResponseCode(),
						String.format("Moved to: %s", newUrl));
				urlConn.disconnect();
				if (retryAfter > 0) {
					backoffAndSleep(retryAfter);
				}
				return attemptDownload(newUrl, retryNumber + 1, report);
			}
			if (is4xx(urlConn.getResponseCode())) {
				// CLIENT ERROR, DO NOT RETRY
				report
					.put(
						REPORT_PREFIX + urlConn.getResponseCode(),
						String
							.format(
								"%s error: %s", requestUrl, urlConn.getResponseMessage()));
				throw new CollectorException("4xx error: request will not be repeated. " + report);
			}
			if (is5xx(urlConn.getResponseCode())) {
				// SERVER SIDE ERRORS RETRY ONLY on 503
				switch (urlConn.getResponseCode()) {
					case HttpURLConnection.HTTP_UNAVAILABLE:
						if (retryAfter > 0) {
							log
								.warn(
									requestUrl + " - waiting and repeating request after suggested retry-after "
										+ retryAfter + " sec.");
							backoffAndSleep(retryAfter * 1000);
						} else {
							log
								.warn(
									requestUrl + " - waiting and repeating request after default delay of "
										+ getClientParams().getRetryDelay() + " sec.");
							backoffAndSleep(retryNumber * getClientParams().getRetryDelay() * 1000);
						}
						report.put(REPORT_PREFIX + urlConn.getResponseCode(), requestUrl);
						urlConn.disconnect();
						return attemptDownload(requestUrl, retryNumber + 1, report);
					default:
						report
							.put(
								REPORT_PREFIX + urlConn.getResponseCode(),
								String
									.format(
										"%s Error: %s", requestUrl, urlConn.getResponseMessage()));
						throw new CollectorException(urlConn.getResponseCode() + " error " + report);
				}
			}
			throw new CollectorException(
				String
					.format(
						"Unexpected status code: %s errors: %s", urlConn.getResponseCode(),
						MAPPER.writeValueAsString(report)));
		} catch (MalformedURLException | SocketException | UnknownHostException e) {
			log.error(e.getMessage(), e);
			report.put(e.getClass().getName(), e.getMessage());
			throw new CollectorException(e.getMessage(), e);
		} catch (SocketTimeoutException e) {
			log.error(e.getMessage(), e);
			report.put(e.getClass().getName(), e.getMessage());
			backoffAndSleep(getClientParams().getRetryDelay() * retryNumber * 1000);
			return attemptDownload(requestUrl, retryNumber + 1, report);
		}
	}

	private void logHeaderFields(final HttpURLConnection urlConn) throws IOException {
		log.debug("StatusCode: " + urlConn.getResponseMessage());

		for (Map.Entry<String, List<String>> e : urlConn.getHeaderFields().entrySet()) {
			if (e.getKey() != null) {
				for (String v : e.getValue()) {
					log.debug("  key: " + e.getKey() + " - value: " + v);
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
			if ((key != null) && key.equalsIgnoreCase(HttpHeaders.RETRY_AFTER) && (headerMap.get(key).size() > 0)
				&& NumberUtils.isCreatable(headerMap.get(key).get(0))) {
				return Integer.parseInt(headerMap.get(key).get(0)) + 10;
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
