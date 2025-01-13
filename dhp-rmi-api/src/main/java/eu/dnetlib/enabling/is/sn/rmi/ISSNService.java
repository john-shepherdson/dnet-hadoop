
package eu.dnetlib.enabling.is.sn.rmi;

import java.util.List;

import eu.dnetlib.common.rmi.BaseService;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;
import jakarta.xml.ws.wsaddressing.W3CEndpointReference;

@WebService(targetNamespace = "http://services.dnetlib.eu/")
public interface ISSNService extends BaseService {

	/**
	 * fossil.
	 *
	 * @param topic
	 * @return
	 * @throws ISSNException
	 */
	String getCurrentMessage(@WebParam(name = "topic") String topic) throws ISSNException;

	/**
	 * puts a subcription in a paused state. paused subscription are not notified even when triggered.
	 *
	 * @param subscrId subscription identifier
	 * @return returns false if the subscription is already paused.
	 * @throws ISSNException may happen
	 */
	boolean pauseSubscription(@WebParam(name = "subscrId") String subscrId) throws ISSNException;

	/**
	 * Used to renew the subscription before it expires.
	 *
	 * <p>
	 * In practice it resets the ttl to another value, so it can be used to reset a infinte ttl subscription to a finite
	 * value.
	 * </p>
	 *
	 * @param subscrId        subscription id
	 * @param terminationTime new ttl (from now), or 0 (infinite)
	 * @return true if successful
	 * @throws ISSNException may happen
	 */
	boolean renew(@WebParam(name = "subscrId") String subscrId, @WebParam(name = "terminationTime") int terminationTime)
		throws ISSNException;

	/**
	 * resumes a paused subscription.
	 *
	 * @param subscrId subscription id
	 * @return true if resumed. false if it was not paused.
	 * @throws ISSNException may happen
	 */
	boolean resumeSubscription(@WebParam(name = "subscrId") String subscrId) throws ISSNException;

	/**
	 * @param consumerReference      epr to be called when the notification is triggered
	 * @param topicExpression        topic expression to register
	 * @param initialTerminationTime ttl in seconds (0 = infinite)
	 * @return subscription id
	 * @throws ISSNException may happen
	 */
	String subscribe(
		@WebParam(name = "consumerReference") W3CEndpointReference consumerReference,
		@WebParam(name = "topicExpression") String topicExpression,
		@WebParam(name = "initialTerminationTime") int initialTerminationTime)
		throws ISSNException;

	boolean unsubscribe(@WebParam(name = "subscrId") String subscrId) throws ISSNException;

	/**
	 * fossil.
	 *
	 * @param resourceType
	 * @param profileId
	 * @param profile
	 * @return
	 * @throws ISSNException
	 */
	boolean actionCreatePerformed(
		@WebParam(name = "resourceType") String resourceType,
		@WebParam(name = "profileId") String profileId,
		@WebParam(name = "profile") String profile) throws ISSNException;

	/**
	 * fossil.
	 *
	 * @param resourceType
	 * @param profileId
	 * @param profileBefore
	 * @param profileAfter
	 * @return
	 * @throws ISSNException
	 */
	boolean actionUpdatePerformed(
		@WebParam(name = "resourceType") String resourceType,
		@WebParam(name = "profileId") String profileId,
		@WebParam(name = "profileBefore") String profileBefore,
		@WebParam(name = "profileAfter") String profileAfter) throws ISSNException;

	/**
	 * fossil.
	 *
	 * @param resourceType
	 * @param profileId
	 * @return
	 * @throws ISSNException
	 */
	boolean actionDeletePerformed(@WebParam(name = "resourceType") String resourceType,
		@WebParam(name = "profileId") String profileId)
		throws ISSNException;

	/**
	 * list all subscriptions. Mostly for debug reasons.
	 *
	 * @return list of subscription ids.
	 */
	List<String> listSubscriptions();

}
