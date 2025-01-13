
package eu.dnetlib.enabling.resultset.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebMethod;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;
import jakarta.xml.ws.wsaddressing.W3CEndpointReference;

/**
 * ResultSet service interface.
 * <p>
 * TODO: implement other compatibility methods as needed.
 *
 * @author marko
 */
@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface ResultSetService extends BaseService {
	/**
	 * create a new pull rs.
	 *
	 * @param bdId            bulk data identifier
	 * @param initialPageSize page size for the polling on the server side.
	 * @param expiryTime      RS expiry time
	 * @return
	 */
	W3CEndpointReference createPullRSEPR(
		@WebParam(name = "dataProviderServiceAddress") W3CEndpointReference dataProviderEPR,
		@WebParam(name = "bdId") String bdId,
		@WebParam(name = "initialPageSize") int initialPageSize,
		@WebParam(name = "expiryTime") int expiryTime,
		@WebParam(name = "styleSheet") String styleSheet,
		@WebParam(name = "keepAliveTime") Integer keepAliveTime,
		@WebParam(name = "total") Integer total);

	/**
	 * create a new pull rs.
	 * <p>
	 * compatibility version
	 *
	 * @param bdId            bulk data identifier
	 * @param initialPageSize page size for the polling on the server side.
	 * @param expiryTime      RS expiry time
	 * @return
	 */
	W3CEndpointReference createPullRS(
		@WebParam(name = "dataProviderServiceAddress") String dataProviderServiceAddress,
		@WebParam(name = "bdId") String bdId,
		@WebParam(name = "initialPageSize") int initialPageSize,
		@WebParam(name = "expiryTime") int expiryTime,
		@WebParam(name = "styleSheet") String styleSheet,
		@WebParam(name = "keepAliveTime") Integer keepAliveTime,
		@WebParam(name = "total") Integer total);

	/**
	 * close a result set. A closed resultset is guaranteed not to grow.
	 *
	 * @param rsId
	 */
	void closeRS(@WebParam(name = "rsId") String rsId);

	/**
	 * get one 'page' of results.
	 * <p>
	 * TODO: define how results are returned when the range is not present in the result set.
	 *
	 * @param fromPosition counting from 1
	 * @param toPosition   included
	 * @param requestMode
	 * @return a page of data
	 */
	List<String> getResult(
		@WebParam(name = "rsId") String rsId,
		@WebParam(name = "fromPosition") int fromPosition,
		@WebParam(name = "toPosition") int toPosition,
		@WebParam(name = "requestMode") String requestMode) throws ResultSetException;

	/**
	 * get the number of result elements present in the resultset.
	 *
	 * @param rsId result set identifier
	 * @return number of results available in the resultset
	 * @throws ResultSetException
	 */
	int getNumberOfElements(@WebParam(name = "rsId") String rsId) throws ResultSetException;

	/**
	 * create a new push resultset.
	 *
	 * @param expiryTime    RS expiry time
	 * @param keepAliveTime keep alive time
	 * @return epr of new resultset
	 * @throws ResultSetException
	 */
	W3CEndpointReference createPushRS(@WebParam(name = "expiryTime") int expiryTime,
		@WebParam(name = "keepAliveTime") int keepAliveTime)
		throws ResultSetException;

	/**
	 * add new data to a push resultset.
	 *
	 * @param rsId     resultset id
	 * @param elements list of elements to be addded
	 * @return dummy value
	 * @throws ResultSetException
	 */
	String populateRS(@WebParam(name = "rsId") String rsId, @WebParam(name = "elements") List<String> elements)
		throws ResultSetException;

	/**
	 * return current status of a resultset.
	 *
	 * @param rsId resultset id
	 * @return status
	 * @throws ResultSetException
	 */
	String getRSStatus(@WebParam(name = "rsId") String rsId) throws ResultSetException;

	/**
	 * read a resultset property.
	 *
	 * @param rsId resultset id
	 * @param name property value
	 * @return property value
	 * @throws ResultSetException
	 */
	String getProperty(@WebParam(name = "rsId") String rsId, @WebParam(name = "name") String name)
		throws ResultSetException;

	@WebMethod(operationName = "identify")
	String identify();

}
