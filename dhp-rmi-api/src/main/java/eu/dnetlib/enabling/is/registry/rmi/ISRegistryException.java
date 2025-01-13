
package eu.dnetlib.enabling.is.registry.rmi;

import eu.dnetlib.common.rmi.RMIException;

public class ISRegistryException extends RMIException {

	/**
	 *
	 */
	private static final long serialVersionUID = -3347405941287624771L;

	public ISRegistryException(Throwable e) {
		super(e);
	}

	public ISRegistryException(String string) {
		super(string);
	}

	public ISRegistryException(String string, Throwable e) {
		super(string, e);
	}

}
