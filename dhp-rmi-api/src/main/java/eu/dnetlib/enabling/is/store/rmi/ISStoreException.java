
package eu.dnetlib.enabling.is.store.rmi;

import eu.dnetlib.common.rmi.RMIException;

public class ISStoreException extends RMIException {

	/**
	 *
	 */
	private static final long serialVersionUID = 8683126829156096420L;

	public ISStoreException(Throwable e) {
		super(e);
	}

	public ISStoreException(String message, Throwable e) {
		super(message, e);
	}

	public ISStoreException(String message) {
		super(message);
	}

}
