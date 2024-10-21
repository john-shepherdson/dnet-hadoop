
package eu.dnetlib.enabling.is.registry.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebService;

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface ISRegistryService extends BaseService {

	boolean addOrUpdateResourceType(String resourceType, String resourceSchema) throws ISRegistryException;

	boolean addResourceType(String resourceType, String resourceSchema) throws ISRegistryException;

	boolean deleteProfile(String profId) throws ISRegistryException;

	@Deprecated
	boolean deleteProfiles(List<String> arrayprofId) throws ISRegistryException;

	/**
	 * @param resourceType
	 * @param hierarchical remove subscription topics
	 * @return
	 * @throws ISRegistryException
	 */
	boolean deleteResourceType(String resourceType, Boolean hierarchical) throws ISRegistryException;

	boolean executeXUpdate(String XQuery) throws ISRegistryException;

	String insertProfileForValidation(String resourceType, String resourceProfile) throws ISRegistryException;

	String invalidateProfile(String profId) throws ISRegistryException;

	boolean refreshProfile(String profId, String resourceType) throws ISRegistryException;

	/**
	 * register a XML Profile.
	 *
	 * @param resourceProfile xml profile
	 * @return profile id
	 * @throws ISRegistryException
	 */
	String registerProfile(String resourceProfile) throws ISRegistryException;

	String registerSecureProfile(String resourceProfId, String secureProfId) throws ISRegistryException;

	boolean updateProfile(String profId, String resourceProfile, String resourceType) throws ISRegistryException;

	@Deprecated
	String updateProfileDHN(String resourceProfile) throws ISRegistryException;

	boolean addProfileNode(String profId, String xpath, String node) throws ISRegistryException;

	boolean updateProfileNode(String profId, String xpath, String node) throws ISRegistryException;

	boolean removeProfileNode(String profId, String nodeId) throws ISRegistryException;

	@Deprecated
	boolean updateRegionDescription(String profId, String resourceProfile) throws ISRegistryException;

	String validateProfile(String profId) throws ISRegistryException;

	@Deprecated
	List<String> validateProfiles(List<String> profIds) throws ISRegistryException;

	void addBlackBoardMessage(String profId, String messageId, String message) throws ISRegistryException;

	void replyBlackBoardMessage(String profId, String message) throws ISRegistryException;

	void deleteBlackBoardMessage(String profId, String messageId) throws ISRegistryException;
}
