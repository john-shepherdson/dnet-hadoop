
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.StringReader;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class QueryInformationSystem {

	private ISLookUpService isLookUp;

	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
		" and ($x//context/param[./@name = 'status']/text() = 'all') "
		+
		"  return " +
		"<community> " +
		"{$x//CONFIGURATION/context/@id}" +
		"{$x//CONFIGURATION/context/@label}" +
		"</community>";

	public CommunityMap getCommunityMap()
		throws ISLookUpException, DocumentException {
		return getMap(isLookUp.quickSearchProfile(XQUERY));

	}

	public ISLookUpService getIsLookUp() {
		return isLookUp;
	}

	public void setIsLookUp(ISLookUpService isLookUpService) {
		this.isLookUp = isLookUpService;
	}

	private CommunityMap getMap(List<String> communityMap) throws DocumentException {
		final CommunityMap map = new CommunityMap();

		for (String xml : communityMap) {
			final Document doc;
			doc = new SAXReader().read(new StringReader(xml));
			Element root = doc.getRootElement();
			map.put(root.attribute("id").getValue(), root.attribute("label").getValue());
		}

		return map;
	}

}
