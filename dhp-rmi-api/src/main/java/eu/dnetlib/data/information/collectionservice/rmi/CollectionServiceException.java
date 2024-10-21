
package eu.dnetlib.data.information.collectionservice.rmi;

import eu.dnetlib.common.rmi.RMIException;
import jakarta.xml.ws.WebFault;

@WebFault
public class CollectionServiceException extends RMIException {

	/**
	 *
	 */
	private static final long serialVersionUID = 8094008463553904905L;

	public CollectionServiceException(Throwable e) {
		super(e);
	}

	public CollectionServiceException(String message, Throwable e) {
		super(message, e);
	}

	public CollectionServiceException(String message) {
		super(message);
	}

}
