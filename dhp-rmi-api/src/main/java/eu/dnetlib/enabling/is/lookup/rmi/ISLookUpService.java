
package eu.dnetlib.enabling.is.lookup.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface ISLookUpService extends BaseService {

	Boolean flushCachedResultSets();

	@Deprecated
	String getCollection(@WebParam(name = "profId") String profId, @WebParam(name = "format") String format)
		throws ISLookUpException;

	String retrieveCollection(@WebParam(name = "profId") String profId) throws ISLookUpException;

	String getResourceProfile(@WebParam(name = "profId") String profId)
		throws ISLookUpException;

	String getResourceProfileByQuery(@WebParam(name = "XQuery") String XQuery)
		throws ISLookUpException;

	String getResourceQoSParams(@WebParam(name = "id") String id) throws ISLookUpException;

	String getResourceTypeSchema(@WebParam(name = "resourceType") String resourceType)
		throws ISLookUpException;

	List<String> listCollections(
		@WebParam(name = "format") String format,
		@WebParam(name = "idfather") String idfather,
		@WebParam(name = "owner") String owner) throws ISLookUpException;

	@Deprecated
	List<String> listDHNIDs() throws ISLookUpException;

	List<String> listResourceTypes() throws ISLookUpException;

	@Deprecated
	List<String> listServiceIDs(@WebParam(name = "serviceType") String serviceType) throws ISLookUpException;

	@Deprecated
	List<String> listServiceTypes() throws ISLookUpException;

	/**
	 * Like searchProfile(), but bypassing the resultset. Useful for short xquery results.
	 *
	 * @param xquery xquery to be executed
	 * @return list of strings (never null)
	 * @throws ISLookUpException could happen
	 */
	List<String> quickSearchProfile(@WebParam(name = "XQuery") String xquery) throws ISLookUpException;

}
