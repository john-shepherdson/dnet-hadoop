
package eu.dnetlib.dhp.common.collection;

public class CollectorException extends Exception {

	/** */
	private static final long serialVersionUID = -290723075076039757L;

	public CollectorException() {
		super();
	}

	public CollectorException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CollectorException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public CollectorException(final String message) {
		super(message);
	}

	public CollectorException(final Throwable cause) {
		super(cause);
	}
}
