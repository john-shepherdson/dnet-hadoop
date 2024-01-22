
package eu.dnetlib.dhp.oa.provision.utils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.jetbrains.annotations.NotNull;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;

import eu.dnetlib.dhp.common.api.context.*;
import eu.dnetlib.dhp.common.rest.DNetRestClient;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class ContextMapper extends HashMap<String, ContextDef> implements Serializable {

	private static final long serialVersionUID = 2159682308502487305L;

	private static final String XQUERY = "for $x in //RESOURCE_PROFILE[.//RESOURCE_TYPE/@value='ContextDSResourceType']//*[name()='context' or name()='category' or name()='concept'] return <entry id=\"{$x/@id}\" label=\"{$x/@label|$x/@name}\" name=\"{$x/name()}\" type=\"{$x/@type}\"/>";

	public static ContextMapper fromAPI(final String baseURL) throws Exception {

		final ContextMapper contextMapper = new ContextMapper();

		for (ContextSummary ctx : DNetRestClient.doGET(baseURL + "/contexts", ContextSummaryList.class)) {

			contextMapper.put(ctx.getId(), new ContextDef(ctx.getId(), ctx.getLabel(), "context", ctx.getType()));

			for (CategorySummary cat : DNetRestClient
				.doGET(baseURL + "/context/" + ctx.getId(), CategorySummaryList.class)) {
				contextMapper.put(cat.getId(), new ContextDef(cat.getId(), cat.getLabel(), "category", ""));
				if (cat.isHasConcept()) {
					for (ConceptSummary c : DNetRestClient
						.doGET(baseURL + "/context/category/" + cat.getId(), ConceptSummaryList.class)) {
						contextMapper.put(c.getId(), new ContextDef(c.getId(), c.getLabel(), "concept", ""));
						if (c.isHasSubConcept()) {
							for (ConceptSummary cs : c.getConcepts()) {
								contextMapper.put(cs.getId(), new ContextDef(cs.getId(), cs.getLabel(), "concept", ""));
								if (cs.isHasSubConcept()) {
									for (ConceptSummary css : cs.getConcepts()) {
										contextMapper
											.put(
												css.getId(),
												new ContextDef(css.getId(), css.getLabel(), "concept", ""));
									}
								}
							}
						}
					}
				}
			}
		}
		return contextMapper;
	}

	@Deprecated
	public static ContextMapper fromIS(final String isLookupUrl)
		throws DocumentException, ISLookUpException, SAXException {
		ISLookUpService isLookUp = ISLookupClientFactory.getLookUpService(isLookupUrl);
		StringBuilder sb = new StringBuilder("<ContextDSResources>");
		Joiner.on("").appendTo(sb, isLookUp.quickSearchProfile(XQUERY));
		sb.append("</ContextDSResources>");
		return fromXml(sb.toString());
	}

	@Deprecated
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
