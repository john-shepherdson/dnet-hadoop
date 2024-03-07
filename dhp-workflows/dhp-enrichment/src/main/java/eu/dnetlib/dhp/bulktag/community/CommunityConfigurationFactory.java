
package eu.dnetlib.dhp.bulktag.community;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.xml.sax.SAXException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.dnetlib.dhp.bulktag.criteria.InterfaceAdapter;
import eu.dnetlib.dhp.bulktag.criteria.Selection;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;

/** Created by miriam on 03/08/2018. */
public class CommunityConfigurationFactory {

	private static final Log log = LogFactory.getLog(CommunityConfigurationFactory.class);

	private static final VerbResolver resolver = VerbResolverFactory.newInstance();

	private CommunityConfigurationFactory() {
	}

	public static CommunityConfiguration newInstance(final String xml) throws DocumentException, SAXException {

		log.info(String.format("parsing community configuration from:\n%s", xml));

		final SAXReader reader = new SAXReader();
		reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
		final Document doc = reader.read(new StringReader(xml));

		final Map<String, Community> communities = Maps.newHashMap();

		for (final Object o : doc.selectNodes("//community")) {

			final Node node = (Node) o;

			final Community community = parseCommunity(node);

			if (community.isValid()) {
				communities.put(community.getId(), community);
			}
		}

		log.info(String.format("loaded %s community configuration profiles", communities.size()));
		log.debug(String.format("loaded community configuration:\n%s", communities));

		return new CommunityConfiguration(communities);
	}

	public static CommunityConfiguration fromJson(final String json) {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(Selection.class, new InterfaceAdapter());
		Gson gson = builder.create();
		final CommunityConfiguration conf = gson.fromJson(json, CommunityConfiguration.class);
		log.info(String.format("loaded %s community configuration profiles", conf.size()));
		conf.init();
		log.info("created inverse maps");

		return conf;
	}

	private static Community parseCommunity(final Node node) {

		final Community c = new Community();

		c.setId(node.valueOf("./@id"));

		log.info(String.format("community id: %s", c.getId()));

		c.setSubjects(parseSubjects(node));
		c.setProviders(parseDatasources(node));
		c.setZenodoCommunities(parseZenodoCommunities(node));
		c.setConstraints(parseConstrains(node));
		c.setRemoveConstraints(parseRemoveConstrains(node));
		return c;
	}

	private static SelectionConstraints parseConstrains(Node node) {
		Node advConstsNode = node.selectSingleNode("./advancedConstraints");
		if (advConstsNode == null || StringUtils.isBlank(StringUtils.trim(advConstsNode.getText()))) {
			return new SelectionConstraints();
		}
		SelectionConstraints selectionConstraints = new Gson()
			.fromJson(advConstsNode.getText(), SelectionConstraints.class);

		selectionConstraints.setSelection(resolver);
		log.info("number of selection constraints set " + selectionConstraints.getCriteria().size());
		return selectionConstraints;
	}

	private static SelectionConstraints parseRemoveConstrains(Node node) {
		Node constsNode = node.selectSingleNode("./removeConstraints");
		if (constsNode == null || StringUtils.isBlank(StringUtils.trim(constsNode.getText()))) {
			return new SelectionConstraints();
		}
		SelectionConstraints selectionConstraints = new Gson()
			.fromJson(constsNode.getText(), SelectionConstraints.class);

		selectionConstraints.setSelection(resolver);
		log.info("number of selection constraints set " + selectionConstraints.getCriteria().size());
		return selectionConstraints;
	}

	private static List<String> parseSubjects(final Node node) {

		final List<String> subjects = Lists.newArrayList();

		final List<Node> list = node.selectNodes("./subjects/subject");

		for (Node n : list) {
			log.debug("text of the node " + n.getText());
			subjects.add(StringUtils.trim(n.getText()));
		}
		log.info("size of the subject list " + subjects.size());
		return subjects;
	}

	private static List<Provider> parseDatasources(final Node node) {
		final List<Node> list = node.selectNodes("./datasources/datasource");
		final List<Provider> providerList = new ArrayList<>();
		for (Node n : list) {
			Provider d = new Provider();
			d.setOpenaireId(n.selectSingleNode("./openaireId").getText());
			d.setSelCriteria(n.selectSingleNode("./selcriteria"), resolver);
			providerList.add(d);
		}
		log.info("size of the datasource list " + providerList.size());
		return providerList;
	}

	private static List<String> parseZenodoCommunities(final Node node) {

		final List<Node> list = node.selectNodes("./zenodocommunities/zenodocommunity");
		final List<String> zenodoCommunityList = new ArrayList<>();
		for (Node n : list) {
//			ZenodoCommunity zc = new ZenodoCommunity();
//			zc.setZenodoCommunityId(n.selectSingleNode("./zenodoid").getText());
//			zc.setSelCriteria(n.selectSingleNode("./selcriteria"));

			zenodoCommunityList.add(n.selectSingleNode("./zenodoid").getText());
		}

		log.info("size of the zenodo community list " + zenodoCommunityList.size());
		return zenodoCommunityList;
	}
}
