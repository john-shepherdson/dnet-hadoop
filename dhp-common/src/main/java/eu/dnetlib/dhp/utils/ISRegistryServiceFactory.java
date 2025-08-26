
package eu.dnetlib.dhp.utils;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.enabling.is.registry.rmi.ISRegistryService;

public class ISRegistryServiceFactory {

	private static final Logger log = LoggerFactory.getLogger(ISRegistryServiceFactory.class);

	private static final int requestTimeout = 60000 * 10;
	private static final int connectTimeout = 60000 * 10;

	private ISRegistryServiceFactory() {
	}

	public static ISRegistryService getRegistryServiceService(final String serviceUrl) {
		return getServiceStub(ISRegistryService.class, serviceUrl);
	}

	@SuppressWarnings("unchecked")
	private static <T> T getServiceStub(final Class<T> clazz, final String endpoint) {
		log.info("creating {} stub from {}", clazz.getName(), endpoint);
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
					"setting connectTimeout to {}, requestTimeout to {} for service {}",
					connectTimeout,
					requestTimeout,
					clazz.getCanonicalName());

			policy.setConnectionTimeout(connectTimeout);
			policy.setReceiveTimeout(requestTimeout);
			conduit.setClient(policy);
		}

		return service;
	}
}
