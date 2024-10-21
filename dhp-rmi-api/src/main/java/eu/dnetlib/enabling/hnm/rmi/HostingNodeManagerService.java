
package eu.dnetlib.enabling.hnm.rmi;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;

/**
 * The HostingNodeManager Service is used to ...
 */

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface HostingNodeManagerService extends BaseService {
	String echo(@WebParam(name = "s") String s);
}
