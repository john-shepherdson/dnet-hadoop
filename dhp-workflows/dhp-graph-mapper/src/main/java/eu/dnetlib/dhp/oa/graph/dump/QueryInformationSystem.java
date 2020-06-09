
package eu.dnetlib.dhp.oa.graph.dump;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryInformationSystem {
	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') " +
			"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
			"  return " +
			"<community> " +
			"{$x//CONFIGURATION/context/@id}" +
			"{$x//CONFIGURATION/context/@label}" +
			"</community>";



	public static Map<String,String> getCommunityMap(final String isLookupUrl)
		throws ISLookUpException, DocumentException {
		ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
		final List<String> res = isLookUp.quickSearchProfile(XQUERY);

		final Map<String, String> communityMap = new HashMap<>();

		res.stream().forEach(xml -> {
			final Document doc;
			try {
				doc = new SAXReader().read(new StringReader(xml));
				Element root = doc.getRootElement();
				communityMap.put(root.attribute("id").getValue(), root.attribute("label").getValue());
			} catch (DocumentException e) {
				e.printStackTrace();
			}


		});

		return communityMap;

	}






}
