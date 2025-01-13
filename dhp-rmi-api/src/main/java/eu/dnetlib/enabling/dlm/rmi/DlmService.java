
package eu.dnetlib.enabling.dlm.rmi;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebService;

/**
 * Distributed lock manager. Currently is used mostly to start the underlying lock manager (e.g. zookeeper) and let
 * client interface directly with it.
 *
 * <p>The DLM service profile contains the entry point of the underlying locking service.</p>
 *
 * @author marko
 */
@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface DlmService extends BaseService {
}
