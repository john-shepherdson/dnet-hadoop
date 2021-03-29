
package eu.dnetlib.dhp.collection;

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

	public HttpClientParams() {
		this(_maxNumberOfRetry, _requestDelay, _retryDelay, _connectTimeOut, _readTimeOut);
	}

	public HttpClientParams(int maxNumberOfRetry, int requestDelay, int retryDelay, int connectTimeOut,
		int readTimeOut) {
		this.maxNumberOfRetry = maxNumberOfRetry;
		this.requestDelay = requestDelay;
		this.retryDelay = retryDelay;
		this.connectTimeOut = connectTimeOut;
		this.readTimeOut = readTimeOut;
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

}
