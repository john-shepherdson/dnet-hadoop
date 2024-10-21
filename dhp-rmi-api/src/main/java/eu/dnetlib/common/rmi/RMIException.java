
package eu.dnetlib.common.rmi;

/**
 * All RMI exception thrown from the service remote method invocation interfaces inherit this class
 *
 * @author marko
 */
abstract public class RMIException extends Exception { // NOPMD

	/**
	 *
	 */
	private static final long serialVersionUID = 428841258652765265L;

	public RMIException(final Throwable exception) {
		super(exception);
	}

	public RMIException(final String string) {
		super(string);
	}

	public RMIException(final String string, final Throwable exception) {
		super(string, exception);
	}
}
