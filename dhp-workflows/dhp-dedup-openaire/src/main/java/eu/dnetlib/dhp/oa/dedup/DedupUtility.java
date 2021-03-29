
package eu.dnetlib.dhp.oa.dedup;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.Normalizer;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.google.common.collect.Sets;
import com.wcohen.ss.JaroWinkler;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.clustering.BlacklistAwareClusteringCombiner;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.model.Person;
import scala.Tuple2;

public class DedupUtility {

	public static Map<String, LongAccumulator> constructAccumulator(
		final DedupConfig dedupConf, final SparkContext context) {

		Map<String, LongAccumulator> accumulators = new HashMap<>();

		String acc1 = String.format("%s::%s", dedupConf.getWf().getEntityType(), "records per hash key = 1");
		accumulators.put(acc1, context.longAccumulator(acc1));
		String acc2 = String
			.format(
				"%s::%s",
				dedupConf.getWf().getEntityType(), "missing " + dedupConf.getWf().getOrderField());
		accumulators.put(acc2, context.longAccumulator(acc2));
		String acc3 = String
			.format(
				"%s::%s",
				dedupConf.getWf().getEntityType(),
				String
					.format(
						"Skipped records for count(%s) >= %s",
						dedupConf.getWf().getOrderField(), dedupConf.getWf().getGroupMaxSize()));
		accumulators.put(acc3, context.longAccumulator(acc3));
		String acc4 = String.format("%s::%s", dedupConf.getWf().getEntityType(), "skip list");
		accumulators.put(acc4, context.longAccumulator(acc4));
		String acc5 = String.format("%s::%s", dedupConf.getWf().getEntityType(), "dedupSimilarity (x2)");
		accumulators.put(acc5, context.longAccumulator(acc5));
		String acc6 = String
			.format(
				"%s::%s", dedupConf.getWf().getEntityType(), "d < " + dedupConf.getWf().getThreshold());
		accumulators.put(acc6, context.longAccumulator(acc6));

		return accumulators;
	}

	static Set<String> getGroupingKeys(DedupConfig conf, MapDocument doc) {
		return Sets.newHashSet(BlacklistAwareClusteringCombiner.filterAndCombine(doc, conf));
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
		throws ISLookUpException, DocumentException {
		final ISLookUpService isLookUpService = ISLookupClientFactory.getLookUpService(isLookUpUrl);

		final String xquery = String.format("/RESOURCE_PROFILE[.//DEDUPLICATION/ACTION_SET/@id = '%s']", orchestrator);

		String orchestratorProfile = isLookUpService.getResourceProfileByQuery(xquery);

		final Document doc = new SAXReader().read(new StringReader(orchestratorProfile));

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
}
