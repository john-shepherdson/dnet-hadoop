
package eu.dnetlib.data.mdstore;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebMethod;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;
import jakarta.xml.ws.wsaddressing.W3CEndpointReference;

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface MDStoreService extends BaseService {

	/**
	 * Identifies service and version.
	 *
	 * @return
	 */
	@Override
	String identify();

	/**
	 * Returns ResultSet EPR for delivered mdstore records.
	 *
	 * @param mdId
	 * @param from
	 * @param until
	 * @param recordFilter REGEX on the metadata record
	 * @return ResultSet EPR
	 * @throws MDStoreServiceException
	 */
	W3CEndpointReference deliverMDRecords(@WebParam(name = "mdId")
	final String mdId,
		@WebParam(name = "from")
		final String from,
		@WebParam(name = "until")
		final String until,
		@WebParam(name = "recordsFilter")
		final String recordFilter) throws MDStoreServiceException;

	/**
	 * Deliver single record from selected mdstore.
	 *
	 * @param mdId
	 * @param recordId
	 * @return record
	 * @throws MDStoreServiceException
	 */
	String deliverRecord(@WebParam(name = "mdId")
	final String mdId, @WebParam(name = "recordId")
	final String recordId) throws MDStoreServiceException;

	/**
	 * Returns list of all stored indices.
	 *
	 * @return list of all stored indices
	 */
	List<String> getListOfMDStores() throws MDStoreServiceException;

	List<String> listMDStores(@WebParam(name = "format")
	final String format,
		@WebParam(name = "layout")
		final String layout,
		@WebParam(name = "interpretation")
		final String interpretation) throws MDStoreServiceException;

	W3CEndpointReference bulkDeliverMDRecords(@WebParam(name = "format")
	final String format,
		@WebParam(name = "layout")
		final String layout,
		@WebParam(name = "interpretation")
		final String interpretation) throws MDStoreServiceException;

	/**
	 * Store md records from a result set
	 *
	 * @param mdId
	 * @param rsId
	 * @param storingType
	 * @return returns true immediately.
	 * @throws MDStoreServiceException
	 */
	@Deprecated
	boolean storeMDRecordsFromRS(@WebParam(name = "mdId")
	final String mdId,
		@WebParam(name = "rsId")
		final String rsId,
		@WebParam(name = "storingType")
		final String storingType) throws MDStoreServiceException;

	/**
	 * Gets the size of the mdstore with the given identifier.
	 *
	 * @param mdId identifier of an mdstore
	 * @return the number of records in the store
	 */
	@WebMethod(operationName = "size")
	int size(@WebParam(name = "mdId")
	final String mdId) throws MDStoreServiceException;

	/**
	 * Gets the sum of records stored in all mdstore with the given format, layout , interpretation
	 *
	 * @param format         format
	 * @param layout         layout
	 * @param interpretation interpretation
	 * @return the total number of records in the mdstores of the given type
	 */
	@WebMethod(operationName = "sizeByFormat")
	int size(@WebParam(name = "format")
	final String format,
		@WebParam(name = "layout")
		final String layout,
		@WebParam(name = "interpretation")
		final String interpretation) throws MDStoreServiceException;

}
