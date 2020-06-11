
package eu.dnetlib.dhp.oa.graph.dump;

import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryInformationSystem {

	private ISLookUpService isLookUp;

	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') " +
			"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
			"  return " +
			"<community> " +
			"{$x//CONFIGURATION/context/@id}" +
			"{$x//CONFIGURATION/context/@label}" +
			"</community>";



	public Map<String,String> getCommunityMap()
		throws ISLookUpException {
		return getMap(isLookUp.quickSearchProfile(XQUERY));

	}

	public ISLookUpService getIsLookUp() {
		return isLookUp;
	}

	public void setIsLookUp(ISLookUpService isLookUpService) {
		this.isLookUp = isLookUpService;
	}

public void set(String isLookUpUrl){
		isLookUpUrl = get(isLookUpUrl);
}
	public ISLookUpService
	private static Map<String, String> getMap(List<String> communityMap) {
		final Map<String, String> map = new HashMap<>();

		communityMap.stream().forEach(xml -> {
			final Document doc;
			try {
				doc = new SAXReader().read(new StringReader(xml));
				Element root = doc.getRootElement();
				map.put(root.attribute("id").getValue(), root.attribute("label").getValue());
			} catch (DocumentException e) {
				e.printStackTrace();
			}


		});

		return map;
	}

}
