
package eu.dnetlib.dhp.collection.worker.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.dnetlib.dhp.collection.worker.CollectorException;

/**
 * Migrated from https://svn.driver.research-infrastructures.eu/driver/dnet45/modules/dnet-modular-collector-service/trunk/src/main/java/eu/dnetlib/data/collector/plugins/HttpConnector.java
 *
 * @author jochen, michele, andrea, alessia
 */
public class HttpConnector2 {

	private static final Log log = LogFactory.getLog(HttpConnector.class);

	private int maxNumberOfRetry = 6;
	private int defaultDelay = 120; // seconds
	private int readTimeOut = 120; // seconds

	private String responseType = null;

	private String userAgent = "Mozilla/5.0 (compatible; OAI; +http://www.openaire.eu)";

	public HttpConnector2() {
		CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
	}

	/**
	 * @see HttpConnector2#getInputSource(java.lang.String, eu.dnetlib.dhp.collection.worker.utils.CollectorPluginErrorLogList)
	 */
	public String getInputSource(final String requestUrl) throws CollectorException {
		return attemptDownlaodAsString(requestUrl, 1, new CollectorPluginErrorLogList());
	}

	/**
	 * @see HttpConnector2#getInputSource(java.lang.String, eu.dnetlib.dhp.collection.worker.utils.CollectorPluginErrorLogList)
	 */
	public InputStream getInputSourceAsStream(final String requestUrl) throws CollectorException {
		return IOUtils.toInputStream(getInputSource(requestUrl));
	}

	/**
	 * Given the URL returns the content via HTTP GET
	 *
	 * @param requestUrl the URL
	 * @param errorLogList the list of errors
	 * @return the content of the downloaded resource
	 * @throws CollectorException when retrying more than maxNumberOfRetry times
	 */
	public String getInputSource(final String requestUrl, CollectorPluginErrorLogList errorLogList)
		throws CollectorException {
		return attemptDownlaodAsString(requestUrl, 1, errorLogList);
	}

	private String attemptDownlaodAsString(final String requestUrl, final int retryNumber,
		final CollectorPluginErrorLogList errorList)
		throws CollectorException {
		try {
			InputStream s = attemptDownload(requestUrl, 1, new CollectorPluginErrorLogList());
			try {
				return IOUtils.toString(s);
			} catch (IOException e) {
				log.error("error while retrieving from http-connection occured: " + requestUrl, e);
				Thread.sleep(defaultDelay * 1000);
				errorList.add(e.getMessage());
				return attemptDownlaodAsString(requestUrl, retryNumber + 1, errorList);
			} finally {
				IOUtils.closeQuietly(s);
			}
		} catch (InterruptedException e) {
			throw new CollectorException(e);
		}
	}

	private InputStream attemptDownload(final String requestUrl, final int retryNumber,
		final CollectorPluginErrorLogList errorList)
		throws CollectorException {

		if (retryNumber > maxNumberOfRetry) {
			throw new CollectorException("Max number of retries exceeded. Cause: \n " + errorList);
		}

		log.debug("Downloading " + requestUrl + " - try: " + retryNumber);
		try {
			InputStream input = null;

			try {
				final HttpURLConnection urlConn = (HttpURLConnection) new URL(requestUrl).openConnection();
				urlConn.setInstanceFollowRedirects(false);
				urlConn.setReadTimeout(readTimeOut * 1000);
				urlConn.addRequestProperty("User-Agent", userAgent);

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
					log.debug(String.format("The requested url %s has been moved to %s", requestUrl, newUrl));
					errorList
						.add(
							String
								.format(
									"%s %s %s. Moved to: %s", requestUrl, urlConn.getResponseCode(),
									urlConn.getResponseMessage(), newUrl));
					urlConn.disconnect();
					if (retryAfter > 0)
						Thread.sleep(retryAfter * 1000);
					return attemptDownload(newUrl, retryNumber + 1, errorList);
				}
				if (is4xx(urlConn.getResponseCode())) {
					// CLIENT ERROR, DO NOT RETRY
					errorList
						.add(
							String
								.format(
									"%s error %s: %s", requestUrl, urlConn.getResponseCode(),
									urlConn.getResponseMessage()));
					throw new CollectorException("4xx error: request will not be repeated. " + errorList);
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
								Thread.sleep(retryAfter * 1000);
							} else {
								log
									.warn(
										requestUrl + " - waiting and repeating request after default delay of "
											+ defaultDelay + " sec.");
								Thread.sleep(defaultDelay * 1000);
							}
							errorList.add(requestUrl + " 503 Service Unavailable");
							urlConn.disconnect();
							return attemptDownload(requestUrl, retryNumber + 1, errorList);
						default:
							errorList
								.add(
									String
										.format(
											"%s Error %s: %s", requestUrl, urlConn.getResponseCode(),
											urlConn.getResponseMessage()));
							throw new CollectorException(urlConn.getResponseCode() + " error " + errorList);
					}
				}
				throw new CollectorException(
					String.format("Unexpected status code: %s error %s", urlConn.getResponseCode(), errorList));
			} catch (MalformedURLException | NoRouteToHostException e) {
				errorList.add(String.format("Error: %s for request url: %s", e.getCause(), requestUrl));
				throw new CollectorException(e + "error " + errorList);
			} catch (IOException e) {
				Thread.sleep(defaultDelay * 1000);
				errorList.add(requestUrl + " " + e.getMessage());
				return attemptDownload(requestUrl, retryNumber + 1, errorList);
			}
		} catch (InterruptedException e) {
			throw new CollectorException(e);
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

	private int obtainRetryAfter(final Map<String, List<String>> headerMap) {
		for (String key : headerMap.keySet()) {
			if ((key != null) && key.toLowerCase().equals("retry-after") && (headerMap.get(key).size() > 0)
				&& NumberUtils.isCreatable(headerMap.get(key).get(0))) {
				return Integer
					.parseInt(headerMap.get(key).get(0)) + 10;
			}
		}
		return -1;
	}

	private String obtainNewLocation(final Map<String, List<String>> headerMap) throws CollectorException {
		for (String key : headerMap.keySet()) {
			if ((key != null) && key.toLowerCase().equals("location") && (headerMap.get(key).size() > 0)) {
				return headerMap.get(key).get(0);
			}
		}
		throw new CollectorException("The requested url has been MOVED, but 'location' param is MISSING");
	}

	/**
	 * register for https scheme; this is a workaround and not intended for the use in trusted environments
	 */
	public void initTrustManager() {
		final X509TrustManager tm = new X509TrustManager() {

			@Override
			public void checkClientTrusted(final X509Certificate[] xcs, final String string) {
			}

			@Override
			public void checkServerTrusted(final X509Certificate[] xcs, final String string) {
			}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}
		};
		try {
			final SSLContext ctx = SSLContext.getInstance("TLS");
			ctx.init(null, new TrustManager[] {
				tm
			}, null);
			HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
		} catch (GeneralSecurityException e) {
			log.fatal(e);
			throw new IllegalStateException(e);
		}
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

	public int getMaxNumberOfRetry() {
		return maxNumberOfRetry;
	}

	public void setMaxNumberOfRetry(final int maxNumberOfRetry) {
		this.maxNumberOfRetry = maxNumberOfRetry;
	}

	public int getDefaultDelay() {
		return defaultDelay;
	}

	public void setDefaultDelay(final int defaultDelay) {
		this.defaultDelay = defaultDelay;
	}

	public int getReadTimeOut() {
		return readTimeOut;
	}

	public void setReadTimeOut(final int readTimeOut) {
		this.readTimeOut = readTimeOut;
	}

	public String getResponseType() {
		return responseType;
	}

}
