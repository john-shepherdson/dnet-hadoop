package eu.dnetlib.dhp;

import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import java.util.List;

public class QueryInformationSystem {
    private static final String XQUERY =
            "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType')"
                    + "  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri']"
                    + "  and  $x//CONFIGURATION/context/param[./@name='status']/text() != 'hidden'"
                    + "  return $x//CONFIGURATION/context/@id/string()";

    public static List<String> getCommunityList(final String isLookupUrl) throws ISLookUpException {
        ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
        return isLookUp.quickSearchProfile(XQUERY);
    }
}
