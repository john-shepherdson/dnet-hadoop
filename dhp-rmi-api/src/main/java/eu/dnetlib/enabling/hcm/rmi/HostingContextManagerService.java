
package eu.dnetlib.enabling.hcm.rmi;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebService;

/**
 * Like a HostingNodeManager, but any webapp (web context) can have its own.
 * <p>
 * useful for dispatching notifications shared by all the services local to a single context.
 * </p>
 *
 * @author marko
 * @author antonis
 */
@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface HostingContextManagerService extends BaseService {

}
