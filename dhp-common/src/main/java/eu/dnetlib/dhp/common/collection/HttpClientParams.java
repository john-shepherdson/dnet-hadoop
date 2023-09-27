
package eu.dnetlib.dhp.common.collection;

import java.util.HashMap;
import java.util.Map;

/**
 * Bundles the http connection parameters driving the client behaviour.
 */
public class HttpClientParams {

	// Defaults
	public static int _maxNumberOfRetry = 3;
	public static int _requestDelay = 0; // milliseconds
	public static int _retryDelay = 10; // seconds
	public static int _connectTimeOut = 10; // seconds
	public static int _readTimeOut = 30; // seconds

	public static String _requestMethod = "GET";

	/**
	 * Maximum number of allowed retires before failing
	 */
	private int maxNumberOfRetry;

	/**
	 * Delay between request (Milliseconds)
	 */
	private int requestDelay;

	/**
	 * Time to wait after a failure before retrying (Seconds)
	 */
	private int retryDelay;

	/**
	 * Connect timeout (Seconds)
	 */
	private int connectTimeOut;

	/**
	 * Read timeout (Seconds)
	 */
	private int readTimeOut;

	/**
	 * Custom http headers
	 */
	private Map<String, String> headers;

	/**
	 * Request method (i.e., GET, POST etc)
	 */
	private String requestMethod;


	public HttpClientParams() {
		this(_maxNumberOfRetry, _requestDelay, _retryDelay, _connectTimeOut, _readTimeOut, new HashMap<>(), _requestMethod);
	}

	public HttpClientParams(int maxNumberOfRetry, int requestDelay, int retryDelay, int connectTimeOut,
		int readTimeOut, Map<String, String> headers, String requestMethod) {
		this.maxNumberOfRetry = maxNumberOfRetry;
		this.requestDelay = requestDelay;
		this.retryDelay = retryDelay;
		this.connectTimeOut = connectTimeOut;
		this.readTimeOut = readTimeOut;
		this.headers = headers;
		this.requestMethod = requestMethod;
	}

	public int getMaxNumberOfRetry() {
		return maxNumberOfRetry;
	}

	public void setMaxNumberOfRetry(int maxNumberOfRetry) {
		this.maxNumberOfRetry = maxNumberOfRetry;
	}

	public int getRequestDelay() {
		return requestDelay;
	}

	public void setRequestDelay(int requestDelay) {
		this.requestDelay = requestDelay;
	}

	public int getRetryDelay() {
		return retryDelay;
	}

	public void setRetryDelay(int retryDelay) {
		this.retryDelay = retryDelay;
	}

	public void setConnectTimeOut(int connectTimeOut) {
		this.connectTimeOut = connectTimeOut;
	}

	public int getConnectTimeOut() {
		return connectTimeOut;
	}

	public int getReadTimeOut() {
		return readTimeOut;
	}

	public void setReadTimeOut(int readTimeOut) {
		this.readTimeOut = readTimeOut;
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	public String getRequestMethod() {
		return requestMethod;
	}

	public void setRequestMethod(String requestMethod) {
		this.requestMethod = requestMethod;
	}
}
