
package eu.dnetlib.dhp.actionmanager.project;

public class CollectorServiceException extends Exception {

	private static final long serialVersionUID = 7523999812098059764L;

	public CollectorServiceException(String string) {
		super(string);
	}

	public CollectorServiceException(String string, Throwable exception) {
		super(string, exception);
	}

	public CollectorServiceException(Throwable exception) {
		super(exception);
	}

}
