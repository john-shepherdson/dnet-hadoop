
package eu.dnetlib.dhp.oa.dedup;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.xml.sax.SAXException;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;

public class DedupUtility {

	public static final String OPENORGS_ID_PREFIX = "openorgs____";
	public static final String CORDA_ID_PREFIX = "corda";

	private DedupUtility() {
	}

	public static String createDedupRecordPath(
		final String basePath, final String actionSetId, final String entityType) {
		return String.format("%s/%s/%s_deduprecord", basePath, actionSetId, entityType);
	}

	public static String createEntityPath(final String basePath, final String entityType) {
		return String.format("%s/%s", basePath, entityType);
	}

	public static String createSimRelPath(
		final String basePath, final String actionSetId, final String entityType) {
		return String.format("%s/%s/%s_simrel", basePath, actionSetId, entityType);
	}

	public static String createOpenorgsMergeRelsPath(
		final String basePath, final String actionSetId, final String entityType) {
		return String.format("%s/%s/%s_openorgs_mergerels", basePath, actionSetId, entityType);
	}

	public static String createMergeRelPath(
		final String basePath, final String actionSetId, final String entityType) {
		return String.format("%s/%s/%s_mergerel", basePath, actionSetId, entityType);
	}

	public static String createBlockStatsPath(
		final String basePath, final String actionSetId, final String entityType) {
		return String.format("%s/%s/%s_blockstats", basePath, actionSetId, entityType);
	}

	public static List<DedupConfig> getConfigurations(String isLookUpUrl, String orchestrator)
		throws ISLookUpException, DocumentException, SAXException {
		final ISLookUpService isLookUpService = ISLookupClientFactory.getLookUpService(isLookUpUrl);

		final String xquery = String.format("/RESOURCE_PROFILE[.//DEDUPLICATION/ACTION_SET/@id = '%s']", orchestrator);

		String orchestratorProfile = isLookUpService.getResourceProfileByQuery(xquery);

		final SAXReader reader = new SAXReader();
		reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
		final Document doc = reader.read(new StringReader(orchestratorProfile));

		final String actionSetId = doc.valueOf("//DEDUPLICATION/ACTION_SET/@id");
		final List<DedupConfig> configurations = new ArrayList<>();

		for (final Object o : doc.selectNodes("//SCAN_SEQUENCE/SCAN")) {
			configurations.add(loadConfig(isLookUpService, actionSetId, o));
		}

		return configurations;
	}

	private static DedupConfig loadConfig(
		final ISLookUpService isLookUpService, final String actionSetId, final Object o)
		throws ISLookUpException {
		final Element s = (Element) o;
		final String configProfileId = s.attributeValue("id");
		final String conf = isLookUpService
			.getResourceProfileByQuery(
				String
					.format(
						"for $x in /RESOURCE_PROFILE[.//RESOURCE_IDENTIFIER/@value = '%s'] return $x//DEDUPLICATION/text()",
						configProfileId));
		final DedupConfig dedupConfig = DedupConfig.load(conf);
		dedupConfig.getWf().setConfigurationId(actionSetId);
		return dedupConfig;
	}

	public static int compareOpenOrgIds(String o1, String o2) {
		if (o1.contains(OPENORGS_ID_PREFIX) && o2.contains(OPENORGS_ID_PREFIX))
			return o1.compareTo(o2);
		if (o1.contains(CORDA_ID_PREFIX) && o2.contains(CORDA_ID_PREFIX))
			return o1.compareTo(o2);

		if (o1.contains(OPENORGS_ID_PREFIX))
			return -1;
		if (o2.contains(OPENORGS_ID_PREFIX))
			return 1;

		if (o1.contains(CORDA_ID_PREFIX))
			return -1;
		if (o2.contains(CORDA_ID_PREFIX))
			return 1;

		return o1.compareTo(o2);
	}

	public static Relation createSimRel(String source, String target, String entity) {
		final Relation r = new Relation();
		r.setSource(source);
		r.setTarget(target);
		r.setSubRelType("dedupSimilarity");
		r.setRelClass(ModelConstants.IS_SIMILAR_TO);
		r.setDataInfo(new DataInfo());

		switch (entity) {
			case "result":
				r.setRelType(ModelConstants.RESULT_RESULT);
				break;
			case "organization":
				r.setRelType(ModelConstants.ORG_ORG_RELTYPE);
				break;
			default:
				throw new IllegalArgumentException("unmanaged entity type: " + entity);
		}
		return r;
	}

}
