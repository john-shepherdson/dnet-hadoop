
package eu.dnetlib.data.utility.objectpackaging.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;
import jakarta.xml.ws.wsaddressing.W3CEndpointReference;

/**
 * The Object Packaging Service is used to combine the records spread
 * into one information package, namely an Object Record.
 */

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface ObjectPackagingService extends BaseService {
	/**
	 * Return the EPR of the resultSet containing the generated packages
	 *
	 * @param eprs     A list of EPRs used to access the input resultSets. ResultSets MUST be ordered using an order key identified by xpath_ID
	 * @param xpath_ID A valid xpath, used to access the ordered ID of the elements of the input resultSets.
	 * @return EPR of the generated resultset
	 */
	W3CEndpointReference generatePackages(@WebParam(name = "eprs") List<W3CEndpointReference> eprs,
		@WebParam(name = "xpath_ID") String xpath_ID) throws ObjectPackagingException;

	/**
	 * Return the EPR of the resultSet containing the unpackaged element
	 *
	 * @param epr The epr used to access the resultset that contains input packages, packages are xml record in this format: <objectRecord><elem>REC1</elem><elem>REC2</elem><elem>REC3</elem></objectRecord>
	 * @return EPR of the generated resultset
	 */
	W3CEndpointReference splitPackages(@WebParam(name = "epr") W3CEndpointReference epr)
		throws ObjectPackagingException;

}
