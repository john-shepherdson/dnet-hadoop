
package eu.dnetlib.data.utility.objectpackaging.rmi;

import eu.dnetlib.common.rmi.RMIException;
import jakarta.xml.ws.WebFault;

@WebFault
public class ObjectPackagingException extends RMIException {

	private static final long serialVersionUID = 3468254939586031822L;

	/**
	 *
	 */

	public ObjectPackagingException(Throwable e) {
		super(e);
	}

	public ObjectPackagingException(String message, Throwable e) {
		super(message, e);
	}

	public ObjectPackagingException(String message) {
		super(message);
	}

}
