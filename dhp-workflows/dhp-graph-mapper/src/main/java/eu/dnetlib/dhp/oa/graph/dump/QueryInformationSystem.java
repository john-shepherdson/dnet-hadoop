
package eu.dnetlib.dhp.oa.graph.dump;

import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

import java.util.List;

public class QueryInformationSystem {
	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') " +
			"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
			"  return " +
			"<community> " +
			"{$x//CONFIGURATION/context/@id}" +
			"{$x//CONFIGURATION/context/@label}" +
			"</community>";



	public List<String> getCommunityMap(final String isLookupUrl)
		throws ISLookUpException {
		ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
		return isLookUp.quickSearchProfile(XQUERY);

	}






}
