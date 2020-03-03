package eu.dnetlib.dhp;

import com.google.common.base.Joiner;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.dom4j.DocumentException;

import java.util.List;


public class QueryInformationSystem {
    private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') " +
            "  let $subj := $x//CONFIGURATION/context/param[./@name='subject']/text() " +
            "  let $datasources := $x//CONFIGURATION/context/category[./@id=concat($x//CONFIGURATION/context/@id,'::contentproviders')]/concept  " +
            "  let $organizations := $x//CONFIGURATION/context/category[./@id=concat($x//CONFIGURATION/context/@id,'::resultorganizations')]/concept  " +
            "  let $communities := $x//CONFIGURATION/context/category[./@id=concat($x//CONFIGURATION/context/@id,'::zenodocommunities')]/concept  " +
            "  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri']  " +
            "  return  " +
            "  <community>  " +
            "  { $x//CONFIGURATION/context/@id}  " +
            "  <subjects>  " +
            "  {for $y in tokenize($subj,',')  " +
            "  return  " +
            "  <subject>{$y}</subject>}  " +
            "  </subjects>  " +
            "  <datasources>  " +
            "  {for $d in $datasources  " +
            "  where $d/param[./@name='enabled']/text()='true'  " +
            "  return  " +
            "  <datasource>  " +
            "  <openaireId>  " +
            "  {$d//param[./@name='openaireId']/text()}  " +
            "  </openaireId>  " +
            "  <selcriteria>  " +
            "  {$d/param[./@name='selcriteria']/text()}  " +
            "  </selcriteria>  " +
            "  </datasource> } " +
            "  </datasources>  " +
            "  <zenodocommunities>  " +
            "  {for $zc in $communities  " +
            "  return  " +
            "  <zenodocommunity>  " +
            "  <zenodoid>  " +
            "  {$zc/param[./@name='zenodoid']/text()} " +
            "  </zenodoid> " +
            "  <selcriteria> " +
            "  {$zc/param[./@name='selcriteria']/text()} " +
            "  </selcriteria> " +
            "  </zenodocommunity>} " +
            "  </zenodocommunities>  " +
            "  </community>";

    public static CommunityConfiguration getCommunityConfiguration(final String isLookupUrl) throws ISLookUpException, DocumentException {
        ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
        final List<String> res = isLookUp.quickSearchProfile(XQUERY);

        final String xmlConf = "<communities>" + Joiner.on(" ").join(res) + "</communities>";


        return CommunityConfigurationFactory.newInstance(xmlConf);

    }


}
