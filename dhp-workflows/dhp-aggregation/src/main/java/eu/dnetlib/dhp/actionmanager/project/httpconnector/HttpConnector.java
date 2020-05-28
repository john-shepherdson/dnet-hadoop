
package eu.dnetlib.dhp.actionmanager.project.httpconnector;

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

/**
 * @author jochen, michele, andrea
 */
public class HttpConnector {

	private static final Log log = LogFactory.getLog(HttpConnector.class);

	private int maxNumberOfRetry = 6;
	private int defaultDelay = 120; // seconds
	private int readTimeOut = 120; // seconds

	private String responseType = null;

	private String userAgent = "Mozilla/5.0 (compatible; OAI; +http://www.openaire.eu)";

	public HttpConnector() {
		CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
	}

	/**
	 * Given the URL returns the content via HTTP GET
	 *
	 * @param requestUrl the URL
	 * @return the content of the downloaded resource
	 * @throws CollectorServiceException when retrying more than maxNumberOfRetry times
	 */
	public String getInputSource(final String requestUrl) throws CollectorServiceException {
		return attemptDownlaodAsString(requestUrl, 1, new CollectorPluginErrorLogList());
	}

	/**
	 * Given the URL returns the content as a stream via HTTP GET
	 *
	 * @param requestUrl the URL
	 * @return the content of the downloaded resource as InputStream
	 * @throws CollectorServiceException when retrying more than maxNumberOfRetry times
	 */
	public InputStream getInputSourceAsStream(final String requestUrl) throws CollectorServiceException {
		return attemptDownload(requestUrl, 1, new CollectorPluginErrorLogList());
	}

	private String attemptDownlaodAsString(final String requestUrl, final int retryNumber,
		final CollectorPluginErrorLogList errorList)
		throws CollectorServiceException {
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
			throw new CollectorServiceException(e);
		}
	}

	private InputStream attemptDownload(final String requestUrl, final int retryNumber,
		final CollectorPluginErrorLogList errorList)
		throws CollectorServiceException {

		if (retryNumber > maxNumberOfRetry) {
			throw new CollectorServiceException("Max number of retries exceeded. Cause: \n " + errorList);
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
				if (retryAfter > 0 && urlConn.getResponseCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
					log.warn("waiting and repeating request after " + retryAfter + " sec.");
					Thread.sleep(retryAfter * 1000);
					errorList.add("503 Service Unavailable");
					urlConn.disconnect();
					return attemptDownload(requestUrl, retryNumber + 1, errorList);
				} else if ((urlConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM)
					|| (urlConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP)) {
					final String newUrl = obtainNewLocation(urlConn.getHeaderFields());
					log.debug("The requested url has been moved to " + newUrl);
					errorList
						.add(
							String
								.format(
									"%s %s. Moved to: %s", urlConn.getResponseCode(), urlConn.getResponseMessage(),
									newUrl));
					urlConn.disconnect();
					return attemptDownload(newUrl, retryNumber + 1, errorList);
				} else if (urlConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
					log
						.error(
							String
								.format("HTTP error: %s %s", urlConn.getResponseCode(), urlConn.getResponseMessage()));
					Thread.sleep(defaultDelay * 1000);
					errorList.add(String.format("%s %s", urlConn.getResponseCode(), urlConn.getResponseMessage()));
					urlConn.disconnect();
					return attemptDownload(requestUrl, retryNumber + 1, errorList);
				} else {
					input = urlConn.getInputStream();
					responseType = urlConn.getContentType();
					return input;
				}
			} catch (IOException e) {
				log.error("error while retrieving from http-connection occured: " + requestUrl, e);
				Thread.sleep(defaultDelay * 1000);
				errorList.add(e.getMessage());
				return attemptDownload(requestUrl, retryNumber + 1, errorList);
			}
		} catch (InterruptedException e) {
			throw new CollectorServiceException(e);
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

	private String obtainNewLocation(final Map<String, List<String>> headerMap) throws CollectorServiceException {
		for (String key : headerMap.keySet()) {
			if ((key != null) && key.toLowerCase().equals("location") && (headerMap.get(key).size() > 0)) {
				return headerMap.get(key).get(0);
			}
		}
		throw new CollectorServiceException("The requested url has been MOVED, but 'location' param is MISSING");
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
