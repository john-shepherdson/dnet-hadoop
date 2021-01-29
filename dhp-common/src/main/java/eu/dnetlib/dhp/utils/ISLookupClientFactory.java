
package eu.dnetlib.dhp.utils;

import java.util.Map;

import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
// import javax.xml.ws.BindingProvider;  // deprecated since v9 and removed in v11 (https://docs.oracle.com/javase/9/docs/api/java.xml.bind-summary.html#packages.summary)
import jakarta.xml.ws.BindingProvider;

public class ISLookupClientFactory {

	private static final Logger log = LoggerFactory.getLogger(ISLookupClientFactory.class);

	private static int requestTimeout = 60000 * 10;
	private static int connectTimeout = 60000 * 10;

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

		if (service instanceof BindingProvider) {
			log
				.info(
					"setting timeouts for {} to requestTimeout: {}, connectTimeout: {}",
					BindingProvider.class.getName(), requestTimeout, connectTimeout);

			Map<String, Object> requestContext = ((BindingProvider) service).getRequestContext();

			requestContext.put("com.sun.xml.internal.ws.request.timeout", requestTimeout);
			requestContext.put("com.sun.xml.internal.ws.connect.timeout", connectTimeout);
			requestContext.put("com.sun.xml.ws.request.timeout", requestTimeout);
			requestContext.put("com.sun.xml.ws.connect.timeout", connectTimeout);
//			requestContext.put("javax.xml.ws.client.receiveTimeout", requestTimeout);
//			requestContext.put("javax.xml.ws.client.connectionTimeout", connectTimeout);
			requestContext.put("jakarta.xml.ws.client.receiveTimeout", requestTimeout);
			requestContext.put("jakarta.xml.ws.client.connectionTimeout", connectTimeout);
		}

		return service;
	}
}
