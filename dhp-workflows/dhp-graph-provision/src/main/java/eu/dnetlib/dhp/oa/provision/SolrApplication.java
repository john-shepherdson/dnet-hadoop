
package eu.dnetlib.dhp.oa.provision;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public abstract class SolrApplication {

	private static final Logger log = LoggerFactory.getLogger(SolrApplication.class);

	protected static final String LAYOUT = "index";
	protected static final String INTERPRETATION = "openaire";
	protected static final String SEPARATOR = "-";
	protected static final String DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss'Z'";

	/**
	 * Method retrieves from the information system the zookeeper quorum of the Solr server
	 *
	 * @param isLookup
	 * @return the zookeeper quorum of the Solr server
	 * @throws ISLookUpException
	 */
	protected static String getZkHost(ISLookUpService isLookup) throws ISLookUpException {
		return doLookup(
			isLookup,
			"for $x in /RESOURCE_PROFILE[.//RESOURCE_TYPE/@value='IndexServiceResourceType'] return $x//PROTOCOL[./@name='solr']/@address/string()");
	}

	protected static String doLookup(ISLookUpService isLookup, String xquery) throws ISLookUpException {
		log.info(String.format("running xquery: %s", xquery));
		final String res = isLookup.getResourceProfileByQuery(xquery);
		log.info(String.format("got response (100 chars): %s", StringUtils.left(res, 100) + " ..."));
		return res;
	}

}
