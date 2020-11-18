
package eu.dnetlib.dhp.oa.provision.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.oa.provision.ProvisionConstants;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpDocumentNotFoundException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class ISLookupClient {

	private static final Logger log = LoggerFactory.getLogger(ISLookupClient.class);

	private ISLookUpService isLookup;

	public ISLookupClient(ISLookUpService isLookup) {
		this.isLookup = isLookup;
	}

	/**
	 * Method retrieves from the information system the list of fields associated to the given MDFormat name
	 *
	 * @param format the Metadata format name
	 * @return the string representation of the list of fields to be indexed
	 * @throws ISLookUpDocumentNotFoundException
	 * @throws ISLookUpException
	 */
	public String getLayoutSource(final String format)
		throws ISLookUpDocumentNotFoundException, ISLookUpException {
		return doLookup(
			String
				.format(
					"collection('')//RESOURCE_PROFILE[.//RESOURCE_TYPE/@value = 'MDFormatDSResourceType' and .//NAME='%s']//LAYOUT[@name='%s']",
					format, ProvisionConstants.LAYOUT));
	}

	/**
	 * Method retrieves from the information system the openaireLayoutToRecordStylesheet
	 *
	 * @return the string representation of the XSLT contained in the transformation rule profile
	 * @throws ISLookUpDocumentNotFoundException
	 * @throws ISLookUpException
	 */
	public String getLayoutTransformer() throws ISLookUpException {
		return doLookup(
			"collection('/db/DRIVER/TransformationRuleDSResources/TransformationRuleDSResourceType')"
				+ "//RESOURCE_PROFILE[./BODY/CONFIGURATION/SCRIPT/TITLE/text() = 'openaireLayoutToRecordStylesheet']//CODE/node()");
	}

	/**
	 * Method retrieves from the information system the IndexDS profile ID associated to the given MDFormat name
	 *
	 * @param format
	 * @return the IndexDS identifier
	 * @throws ISLookUpException
	 */
	public String getDsId(String format) throws ISLookUpException {
		return doLookup(
			String
				.format(
					"collection('/db/DRIVER/IndexDSResources/IndexDSResourceType')"
						+ "//RESOURCE_PROFILE[./BODY/CONFIGURATION/METADATA_FORMAT/text() = '%s']//RESOURCE_IDENTIFIER/@value/string()",
					format));
	}

	/**
	 * Method retrieves from the information system the zookeeper quorum of the Solr server
	 *
	 * @return the zookeeper quorum of the Solr server
	 * @throws ISLookUpException
	 */
	public String getZkHost() throws ISLookUpException {
		return doLookup(
			"for $x in /RESOURCE_PROFILE[.//RESOURCE_TYPE/@value='IndexServiceResourceType'] return $x//PROTOCOL[./@name='solr']/@address/string()");
	}

	private String doLookup(String xquery) throws ISLookUpException {
		log.info(String.format("running xquery: %s", xquery));
		final String res = getIsLookup().getResourceProfileByQuery(xquery);
		log.info(String.format("got response (100 chars): %s", StringUtils.left(res, 100) + " ..."));
		return res;
	}

	public ISLookUpService getIsLookup() {
		return isLookup;
	}

	public void setIsLookup(ISLookUpService isLookup) {
		this.isLookup = isLookup;
	}

}
