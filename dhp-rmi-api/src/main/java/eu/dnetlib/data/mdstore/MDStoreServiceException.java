
package eu.dnetlib.data.mdstore;

/**
 * General mdstore service exception.
 *
 * @author claudio atzori
 * @version 1.0.0
 */
public class MDStoreServiceException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = -6772977735282310658L;

	public MDStoreServiceException(String s, Throwable e) {
		super(s, e);
	}

	public MDStoreServiceException(String s) {
		super(s);
	}

	public MDStoreServiceException(Throwable e) {
		super(e);
	}

	public MDStoreServiceException() {
		super();
	}

}
