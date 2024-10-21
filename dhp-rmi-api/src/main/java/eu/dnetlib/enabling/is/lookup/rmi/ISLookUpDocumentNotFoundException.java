
package eu.dnetlib.enabling.is.lookup.rmi;

import jakarta.xml.ws.WebFault;

/**
 * Thrown when a given document is not found.
 *
 * @author marko
 */
@WebFault
public class ISLookUpDocumentNotFoundException extends ISLookUpException {

	/**
	 * exception chain + message.
	 *
	 * @param message message
	 * @param e
	 */
	public ISLookUpDocumentNotFoundException(String message, Throwable e) {
		super(message, e);
	}

	/**
	 * exception chain constructor.
	 *
	 * @param e
	 */
	public ISLookUpDocumentNotFoundException(Throwable e) {
		super(e);
	}

	/**
	 * exception message.
	 *
	 * @param message
	 */
	public ISLookUpDocumentNotFoundException(String message) {
		super(message);
	}

	/**
	 *
	 */
	private static final long serialVersionUID = 2295995755165801937L;

}
