
package eu.dnetlib.enabling.is.store.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface ISStoreService extends BaseService {

	boolean createFileColl(@WebParam(name = "fileColl") String fileColl) throws ISStoreException;

	boolean deleteFileColl(@WebParam(name = "fileColl") String fileColl) throws ISStoreException;

	boolean deleteXML(@WebParam(name = "fileName") String fileName, @WebParam(name = "fileColl") String fileColl)
		throws ISStoreException;

	boolean executeXUpdate(@WebParam(name = "query") String query) throws ISStoreException;

	List<String> getFileColls() throws ISStoreException;

	List<String> getFileNames(@WebParam(name = "fileColl") String fileColl) throws ISStoreException;

	String getXML(@WebParam(name = "fileName") String fileName, @WebParam(name = "fileColl") String fileColl)
		throws ISStoreException;

	String getXMLbyQuery(@WebParam(name = "query") String query) throws ISStoreException;

	boolean insertXML(@WebParam(name = "fileName") String fileName, @WebParam(name = "fileColl") String fileColl,
		@WebParam(name = "file") String file)
		throws ISStoreException;

	boolean reindex();

	List<String> quickSearchXML(@WebParam(name = "query") String query) throws ISStoreException;

	boolean sync();

	boolean updateXML(@WebParam(name = "fileName") String fileName, @WebParam(name = "fileColl") String fileColl,
		@WebParam(name = "file") String file)
		throws ISStoreException;

	String backup() throws ISStoreException;

}
