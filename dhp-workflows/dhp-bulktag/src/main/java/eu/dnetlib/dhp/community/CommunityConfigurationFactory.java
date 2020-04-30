
package eu.dnetlib.dhp.community;

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.dnetlib.dhp.selectioncriteria.InterfaceAdapter;
import eu.dnetlib.dhp.selectioncriteria.Selection;
import eu.dnetlib.dhp.selectioncriteria.VerbResolver;
import eu.dnetlib.dhp.selectioncriteria.VerbResolverFactory;

/** Created by miriam on 03/08/2018. */
public class CommunityConfigurationFactory {

	private static final Log log = LogFactory.getLog(CommunityConfigurationFactory.class);

	private static VerbResolver resolver = VerbResolverFactory.newInstance();

	public static CommunityConfiguration newInstance(final String xml) throws DocumentException {

		log.debug(String.format("parsing community configuration from:\n%s", xml));

		final Document doc = new SAXReader().read(new StringReader(xml));

		final Map<String, Community> communities = Maps.newHashMap();

		for (final Object o : doc.selectNodes("//community")) {

			final Node node = (Node) o;

			final Community community = parseCommunity(node);

			if (community.isValid()) {
				communities.put(community.getId(), community);
			}
		}

		log.info(String.format("loaded %s community configuration profiles", communities.size()));
		log.debug(String.format("loaded community configuration:\n%s", communities.toString()));

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
		c.setDatasources(parseDatasources(node));
		c.setZenodoCommunities(parseZenodoCommunities(node));
		return c;
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

	private static List<Datasource> parseDatasources(final Node node) {
		final List<Node> list = node.selectNodes("./datasources/datasource");
		final List<Datasource> datasourceList = new ArrayList<>();
		for (Node n : list) {
			Datasource d = new Datasource();
			d.setOpenaireId(n.selectSingleNode("./openaireId").getText());
			d.setSelCriteria(n.selectSingleNode("./selcriteria"), resolver);
			datasourceList.add(d);
		}
		log.info("size of the datasource list " + datasourceList.size());
		return datasourceList;
	}

	private static List<ZenodoCommunity> parseZenodoCommunities(final Node node) {
		final Node oacommunitynode = node.selectSingleNode("./oacommunity");
		String oacommunity = null;
		if (oacommunitynode != null) {
			String tmp = oacommunitynode.getText();
			if (StringUtils.isNotBlank(tmp))
				oacommunity = tmp;
		}

		final List<Node> list = node.selectNodes("./zenodocommunities/zenodocommunity");
		final List<ZenodoCommunity> zenodoCommunityList = new ArrayList<>();
		for (Node n : list) {
			ZenodoCommunity zc = new ZenodoCommunity();
			zc.setZenodoCommunityId(n.selectSingleNode("./zenodoid").getText());
			zc.setSelCriteria(n.selectSingleNode("./selcriteria"));

			zenodoCommunityList.add(zc);
		}
		if (oacommunity != null) {
			ZenodoCommunity zc = new ZenodoCommunity();
			zc.setZenodoCommunityId(oacommunity);
			zenodoCommunityList.add(zc);
		}
		log.info("size of the zenodo community list " + zenodoCommunityList.size());
		return zenodoCommunityList;
	}
}
