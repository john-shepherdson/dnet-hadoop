package eu.dnetlib.dhp.utils;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;

public class ISLookupClientFactory {

    private static final Log log = LogFactory.getLog(ISLookupClientFactory.class);

    public static ISLookUpService getLookUpService(final String isLookupUrl) {
        return getServiceStub(ISLookUpService.class, isLookupUrl);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getServiceStub(final Class<T> clazz, final String endpoint) {
        log.info(String.format("creating %s stub from %s", clazz.getName(), endpoint));
        final JaxWsProxyFactoryBean jaxWsProxyFactory = new JaxWsProxyFactoryBean();
        jaxWsProxyFactory.setServiceClass(clazz);
        jaxWsProxyFactory.setAddress(endpoint);
        return (T) jaxWsProxyFactory.create();
    }
}
