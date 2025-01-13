
package eu.dnetlib.data.information.publisher.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebMethod;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;
import jakarta.xml.ws.wsaddressing.W3CEndpointReference;

/**
 * Publisher service. Provides access to metadata records and objects.
 *
 * @author marko
 */
@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface PublisherService extends BaseService {

	/**
	 * Get a (metadata) resource by ID.
	 *
	 * @param id
	 * @param format
	 * @param layout
	 * @param interpretation
	 * @return
	 */
	@WebMethod
	String getResourceById(@WebParam(name = "id")
	final String id,
		@WebParam(name = "format")
		final String format,
		@WebParam(name = "layout")
		final String layout,
		@WebParam(name = "interpretation")
		final String interpretation);

	/**
	 * Get (metadata) resources by IDs.
	 *
	 * @param ids
	 * @param format
	 * @param layout
	 * @param interpretation
	 * @return
	 */
	@WebMethod
	W3CEndpointReference getResourcesByIds(@WebParam(name = "ids")
	final List<String> ids,
		@WebParam(name = "format")
		final String format,
		@WebParam(name = "layout")
		final String layout,
		@WebParam(name = "interpretation")
		final String interpretation);
}
