
package eu.dnetlib.dhp.oa.graph.dump.exceptions;

public class NoAvailableEntityTypeException extends Exception {
	public NoAvailableEntityTypeException() {
		super();
	}

	public NoAvailableEntityTypeException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NoAvailableEntityTypeException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public NoAvailableEntityTypeException(final String message) {
		super(message);
	}

	public NoAvailableEntityTypeException(final Throwable cause) {
		super(cause);
	}

}
