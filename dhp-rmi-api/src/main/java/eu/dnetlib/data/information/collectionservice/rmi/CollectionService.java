
package eu.dnetlib.data.information.collectionservice.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;

/**
 * The Collection Service is used to ...
 */

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface CollectionService extends BaseService {

	String getCollection(@WebParam(name = "collId")
	final String collId) throws CollectionServiceException;

	List<String> getCollections(@WebParam(name = "collIds")
	final List<String> collIds) throws CollectionServiceException;

	void updateCollection(@WebParam(name = "coll")
	final String coll) throws CollectionServiceException;

	void deleteCollection(@WebParam(name = "collId")
	final String collId) throws CollectionServiceException;

	String createCollection(@WebParam(name = "coll")
	final String coll) throws CollectionServiceException;
}
