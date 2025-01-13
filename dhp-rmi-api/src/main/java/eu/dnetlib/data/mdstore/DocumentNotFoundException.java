
package eu.dnetlib.data.mdstore;

/**
 * Signals that a metadata record cannot be found in a given MDStore.
 */
public class DocumentNotFoundException extends MDStoreServiceException {

	/**
	 *
	 */
	private static final long serialVersionUID = 5188036989114250548L;

	public DocumentNotFoundException(final String s, final Throwable e) {
		super(s, e);
	}

	public DocumentNotFoundException(final String s) {
		super(s);
	}

	public DocumentNotFoundException(final Throwable e) {
		super(e);
	}

	public DocumentNotFoundException() {
		super();
	}

}
