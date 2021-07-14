
package eu.dnetlib.dhp.utils;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class ISLookupClientFactory {

	private static final Logger log = LoggerFactory.getLogger(ISLookupClientFactory.class);

	private static final int requestTimeout = 60000 * 10;
	private static final int connectTimeout = 60000 * 10;

	public static ISLookUpService getLookUpService(final String isLookupUrl) {
		return getServiceStub(ISLookUpService.class, isLookupUrl);
	}

	@SuppressWarnings("unchecked")
	private static <T> T getServiceStub(final Class<T> clazz, final String endpoint) {
		log.info(String.format("creating %s stub from %s", clazz.getName(), endpoint));
		final JaxWsProxyFactoryBean jaxWsProxyFactory = new JaxWsProxyFactoryBean();
		jaxWsProxyFactory.setServiceClass(clazz);
		jaxWsProxyFactory.setAddress(endpoint);

		final T service = (T) jaxWsProxyFactory.create();

		Client client = ClientProxy.getClient(service);
		if (client != null) {
			HTTPConduit conduit = (HTTPConduit) client.getConduit();
			HTTPClientPolicy policy = new HTTPClientPolicy();

			log
				.info(
					String
						.format(
							"setting connectTimeout to %s, requestTimeout to %s for service %s",
							connectTimeout,
							requestTimeout,
							clazz.getCanonicalName()));

			policy.setConnectionTimeout(connectTimeout);
			policy.setReceiveTimeout(requestTimeout);
			conduit.setClient(policy);
		}

		return service;
	}
}
