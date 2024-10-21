
package eu.dnetlib.enabling.is.sn.rmi;

import eu.dnetlib.common.rmi.RMIException;

public class ISSNException extends RMIException {

	/**
	 *
	 */
	private static final long serialVersionUID = -7384073901457430004L;

	public ISSNException(final Throwable e) {
		super(e);
	}

	public ISSNException(final String message) {
		super(message);
	}

	public ISSNException(final String message, final Throwable e) {
		super(message, e);
	}

}
