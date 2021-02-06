
package eu.dnetlib.dhp.collection.worker;

public class UnknownCollectorPluginException extends Exception {

	/** */
	private static final long serialVersionUID = -290723075076039757L;

	public UnknownCollectorPluginException() {
		super();
	}

	public UnknownCollectorPluginException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public UnknownCollectorPluginException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public UnknownCollectorPluginException(final String message) {
		super(message);
	}

	public UnknownCollectorPluginException(final Throwable cause) {
		super(cause);
	}
}
