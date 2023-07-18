
package eu.dnetlib.dhp.bulktag.community;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.dom4j.DocumentException;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;

import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class QueryInformationSystem {

	public static CommunityConfiguration getCommunityConfiguration(final String isLookupUrl)
		throws ISLookUpException, DocumentException, SAXException, IOException {
		ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
		final List<String> res = isLookUp
			.quickSearchProfile(
				IOUtils
					.toString(
						QueryInformationSystem.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/bulktag/query.xq")));

		final String xmlConf = "<communities>" + Joiner.on(" ").join(res) + "</communities>";

		return CommunityConfigurationFactory.newInstance(xmlConf);
	}
}
