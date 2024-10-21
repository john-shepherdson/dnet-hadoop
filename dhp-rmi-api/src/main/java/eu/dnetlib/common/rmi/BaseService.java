
package eu.dnetlib.common.rmi;

import jakarta.jws.WebMethod;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface BaseService {

	/**
	 * All DRIVER services must implement method notify() in order to communicate with the IS_SN
	 *
	 * @param subsrciptionId
	 * @param topic
	 * @param isId
	 * @param message
	 */
	@WebMethod(operationName = "notify")
	void notify(@WebParam(name = "subscrId") String subscriptionId,
		@WebParam(name = "topic") String topic,
		@WebParam(name = "is_id") String isId,
		@WebParam(name = "message") String message);

	/**
	 * Identifies the service's version. Version syntax: ${NAME}-${MAJOR}.${MINOR}.${MICRO}[-${LABEL}]
	 *
	 * @return the service's version
	 */
	@WebMethod(operationName = "identify")
	String identify();

	void start();

}
