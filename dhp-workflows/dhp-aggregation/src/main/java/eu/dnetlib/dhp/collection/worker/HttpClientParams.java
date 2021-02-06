
package eu.dnetlib.dhp.collection.worker;

/**
 * Bundles the http connection parameters driving the client behaviour.
 */
public class HttpClientParams {

	public static int _maxNumberOfRetry = 3;
	public static int _retryDelay = 10; // seconds
	public static int _connectTimeOut = 10; // seconds
	public static int _readTimeOut = 30; // seconds

	private int maxNumberOfRetry;
	private int retryDelay;
	private int connectTimeOut;
	private int readTimeOut;

	public HttpClientParams() {
		this(_maxNumberOfRetry, _retryDelay, _connectTimeOut, _readTimeOut);
	}

	public HttpClientParams(int maxNumberOfRetry, int retryDelay, int connectTimeOut, int readTimeOut) {
		this.maxNumberOfRetry = maxNumberOfRetry;
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
