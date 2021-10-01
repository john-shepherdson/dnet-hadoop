
package eu.dnetlib.dhp.oa.provision.utils;

import java.io.Serializable;
import java.io.StringReader;
import java.util.HashMap;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;

import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class ContextMapper extends HashMap<String, ContextDef> implements Serializable {

	private static final long serialVersionUID = 2159682308502487305L;

	private static final String XQUERY = "for $x in //RESOURCE_PROFILE[.//RESOURCE_TYPE/@value='ContextDSResourceType']//*[name()='context' or name()='category' or name()='concept'] return <entry id=\"{$x/@id}\" label=\"{$x/@label|$x/@name}\" name=\"{$x/name()}\" type=\"{$x/@type}\"/>";

	public static ContextMapper fromIS(final String isLookupUrl)
		throws DocumentException, ISLookUpException, SAXException {
		ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
		StringBuilder sb = new StringBuilder("<ContextDSResources>");
		Joiner.on("").appendTo(sb, isLookUp.quickSearchProfile(XQUERY));
		sb.append("</ContextDSResources>");
		return fromXml(sb.toString());
	}

	public static ContextMapper fromXml(final String xml) throws DocumentException, SAXException {
		final ContextMapper contextMapper = new ContextMapper();

		final SAXReader reader = new SAXReader();
		reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
		final Document doc = reader.read(new StringReader(xml));
		for (Object o : doc.selectNodes("//entry")) {
			Node node = (Node) o;
			String id = node.valueOf("./@id");
			String label = node.valueOf("./@label");
			String name = node.valueOf("./@name");
			String type = node.valueOf("./@type") + "";

			contextMapper.put(id, new ContextDef(id, label, name, type));
		}
		return contextMapper;
	}
}
