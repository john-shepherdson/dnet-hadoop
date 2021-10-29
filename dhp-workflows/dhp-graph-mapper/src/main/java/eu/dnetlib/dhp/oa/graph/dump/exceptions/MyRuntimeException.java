
package eu.dnetlib.dhp.oa.graph.dump.exceptions;

public class MyRuntimeException extends RuntimeException {

	public MyRuntimeException() {
		super();
	}

	public MyRuntimeException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MyRuntimeException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public MyRuntimeException(final String message) {
		super(message);
	}

	public MyRuntimeException(final Throwable cause) {
		super(cause);
	}

}
