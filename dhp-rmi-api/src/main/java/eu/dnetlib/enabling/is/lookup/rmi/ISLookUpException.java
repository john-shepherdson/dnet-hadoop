
package eu.dnetlib.enabling.is.lookup.rmi;

import eu.dnetlib.common.rmi.RMIException;
import jakarta.xml.ws.WebFault;

@WebFault
public class ISLookUpException extends RMIException {

	/**
	 *
	 */
	private static final long serialVersionUID = -5626136963653382533L;

	public ISLookUpException(Throwable e) {
		super(e);
	}

	public ISLookUpException(String message, Throwable e) {
		super(message, e);
	}

	public ISLookUpException(String message) {
		super(message);
	}

}
