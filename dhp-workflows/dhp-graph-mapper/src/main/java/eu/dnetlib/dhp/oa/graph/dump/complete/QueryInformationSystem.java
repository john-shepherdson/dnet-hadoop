
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.StringReader;
import java.util.*;
import java.util.function.Consumer;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.jetbrains.annotations.NotNull;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.dhp.utils.DHPUtils;

public class QueryInformationSystem {

	private ISLookUpService isLookUp;
	private List<String> contextRelationResult;

	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
		" and $x//context/param[./@name = 'status']/text() = 'all' " +
		"  return " +
		"$x//context";

	private static final String XQUERY_ENTITY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"where $x//context[./@type='community' or ./@type = 'ri'] and $x//context/param[./@name = 'status']/text() = 'all' return "
		+
		"concat(data($x//context/@id) , '@@', $x//context/param[./@name =\"name\"]/text(), '@@', " +
		"$x//context/param[./@name=\"description\"]/text(), '@@', $x//context/param[./@name = \"subject\"]/text(), '@@', "
		+
		"$x//context/param[./@name = \"zenodoCommunity\"]/text(), '@@', $x//context/@type)";

	public void getContextInformation(final Consumer<ContextInfo> consumer) throws ISLookUpException {

		isLookUp
			.quickSearchProfile(XQUERY_ENTITY)
			.forEach(c -> {
				ContextInfo cinfo = new ContextInfo();
				String[] cSplit = c.split("@@");
				cinfo.setId(cSplit[0]);
				cinfo.setName(cSplit[1]);
				cinfo.setDescription(cSplit[2]);
				if (!cSplit[3].trim().equals("")) {
					cinfo.setSubject(Arrays.asList(cSplit[3].split(",")));
				}
				cinfo.setZenodocommunity(cSplit[4]);
				cinfo.setType(cSplit[5]);
				consumer.accept(cinfo);
			});

	}

	public List<String> getContextRelationResult() {
		return contextRelationResult;
	}

	public void setContextRelationResult(List<String> contextRelationResult) {
		this.contextRelationResult = contextRelationResult;
	}

	public ISLookUpService getIsLookUp() {
		return isLookUp;
	}

	public void setIsLookUp(ISLookUpService isLookUpService) {
		this.isLookUp = isLookUpService;
	}

	public void execContextRelationQuery() throws ISLookUpException {
		contextRelationResult = isLookUp.quickSearchProfile(XQUERY);

	}

	public void getContextRelation(final Consumer<ContextInfo> consumer, String category, String prefix) {

		contextRelationResult.forEach(xml -> {
			ContextInfo cinfo = new ContextInfo();
			final Document doc;

			try {

				doc = new SAXReader().read(new StringReader(xml));
				Element root = doc.getRootElement();
				cinfo.setId(root.attributeValue("id"));

				Iterator it = root.elementIterator();
				while (it.hasNext()) {
					Element el = (Element) it.next();
					if (el.getName().equals("category")) {
						String categoryId = el.attributeValue("id");
						categoryId = categoryId.substring(categoryId.lastIndexOf("::") + 2);
						if (categoryId.equals(category)) {
							cinfo.setDatasourceList(getCategoryList(el, prefix));
						}
					}

				}
				consumer.accept(cinfo);
			} catch (DocumentException e) {
				e.printStackTrace();
			}

		});

	}

	@NotNull
	private List<String> getCategoryList(Element el, String prefix) {
		List<String> datasourceList = new ArrayList<>();
		for (Object node : el.selectNodes(".//concept")) {
			String oid = getOpenaireId((Node) node, prefix);
			if (oid != null)
				datasourceList.add(oid);
		}

		return datasourceList;
	}

	private String getOpenaireId(Node el, String prefix) {
		for (Object node : el.selectNodes(".//param")) {
			Node n = (Node) node;
			if (n.valueOf("./@name").equals("openaireId")) {
				return prefix + "|" + n.getText();
			}
		}

		return makeOpenaireId(el, prefix);

	}

	private String makeOpenaireId(Node el, String prefix) {
		String funder = null;
		String grantId = null;
		String funding = null;
		for (Object node : el.selectNodes(".//param")) {
			Node n = (Node) node;
			switch (n.valueOf("./@name")) {
				case "funding":
					funding = n.getText();
					break;
				case "funder":
					funder = n.getText();
					break;
				case "CD_PROJECT_NUMBER":
					grantId = n.getText();
					break;
			}
		}
		String nsp = null;
		switch (funder.toLowerCase()) {
			case "ec":
				if (funding == null) {
					return null;
				}
				if (funding.toLowerCase().startsWith("h2020")) {
					nsp = "corda__h2020::";
				} else {
					nsp = "corda_______::";
				}
				break;
			case "tubitak":
				nsp = "tubitakf____::";
				break;
			case "dfg":
				nsp = "dfgf________::";
				break;
			default:
				nsp = funder.toLowerCase();
				for (int i = funder.length(); i < 12; i++)
					nsp += "_";
				nsp += "::";
		}

		return prefix + "|" + nsp + DHPUtils.md5(grantId);
	}

}
