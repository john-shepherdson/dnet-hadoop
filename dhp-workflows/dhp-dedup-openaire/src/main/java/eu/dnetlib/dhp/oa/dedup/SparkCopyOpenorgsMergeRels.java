
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import net.sf.saxon.ma.trie.Tuple2;

public class SparkCopyOpenorgsMergeRels extends AbstractSparkAction {
	private static final Logger log = LoggerFactory.getLogger(SparkCopyOpenorgsMergeRels.class);
	public static final String PROVENANCE_ACTION_CLASS = "sysimport:dedup";
	public static final String DNET_PROVENANCE_ACTIONS = "dnet:provenanceActions";

	public SparkCopyOpenorgsMergeRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgsMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkCopyOpenorgsMergeRels(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws DocumentException, IOException, ISLookUpException {

		// read oozie parameters
		final String graphBasePath = parser.get("graphBasePath");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");
		final int numPartitions = Optional
			.ofNullable(parser.get("numPartitions"))
			.map(Integer::valueOf)
			.orElse(NUM_PARTITIONS);

		log.info("numPartitions: '{}'", numPartitions);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		log.info("Copying OpenOrgs Merge Rels");

		final String outputPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, "organization");

		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		DedupConfig dedupConf = getConfigurations(isLookUpService, actionSetId).get(0);

		JavaRDD<Relation> rawRels = spark
			.read()
			.textFile(relationPath)
			.map(patchRelFn(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(this::isOpenorgs)
			.filter(this::filterOpenorgsRels);

		JavaRDD<Relation> selfRawRels = rawRels
			.map(r -> r.getSource())
			.distinct()
			.map(s -> rel(s, s, "isSimilarTo", dedupConf));

		log.info("Number of raw Openorgs Relations collected: {}", rawRels.count());

		// turn openorgs isSimilarTo relations into mergerels
		JavaRDD<Relation> mergeRelsRDD = rawRels
			.union(selfRawRels)
			.map(r -> {
				r.setSource(createDedupID(r.getSource())); // create the dedup_id to align it to the openaire dedup
															// format
				return r;
			})
			.flatMap(rel -> {

				List<Relation> mergerels = new ArrayList<>();

				mergerels.add(rel(rel.getSource(), rel.getTarget(), "merges", dedupConf));
				mergerels.add(rel(rel.getTarget(), rel.getSource(), "isMergedIn", dedupConf));

				return mergerels.iterator();
			});

		log.info("Number of Openorgs Merge Relations created: {}", mergeRelsRDD.count());

		spark
			.createDataset(
				mergeRelsRDD.rdd(),
				Encoders.bean(Relation.class))
			.write()
			.mode(SaveMode.Append)
			.parquet(outputPath);
	}

	private static MapFunction<String, Relation> patchRelFn() {
		return value -> {
			final Relation rel = OBJECT_MAPPER.readValue(value, Relation.class);
			if (rel.getDataInfo() == null) {
				rel.setDataInfo(new DataInfo());
			}
			return rel;
		};
	}

	private boolean filterOpenorgsRels(Relation rel) {

		if (rel.getRelClass().equals("isSimilarTo") && rel.getRelType().equals("organizationOrganization")
			&& rel.getSubRelType().equals("dedup"))
			return true;
		return false;
	}

	private boolean isOpenorgs(Relation rel) {

		if (rel.getCollectedfrom() != null) {
			for (KeyValue k : rel.getCollectedfrom()) {
				if (k.getValue() != null && k.getValue().equals("OpenOrgs Database")) {
					return true;
				}
			}
		}
		return false;
	}

	private Relation rel(String source, String target, String relClass, DedupConfig dedupConf) {

		String entityType = dedupConf.getWf().getEntityType();

		Relation r = new Relation();
		r.setSource(source);
		r.setTarget(target);
		r.setRelClass(relClass);
		r.setRelType(entityType + entityType.substring(0, 1).toUpperCase() + entityType.substring(1));
		r.setSubRelType("dedup");

		DataInfo info = new DataInfo();
		info.setDeletedbyinference(false);
		info.setInferred(true);
		info.setInvisible(false);
		info.setInferenceprovenance(dedupConf.getWf().getConfigurationId());
		Qualifier provenanceAction = new Qualifier();
		provenanceAction.setClassid(PROVENANCE_ACTION_CLASS);
		provenanceAction.setClassname(PROVENANCE_ACTION_CLASS);
		provenanceAction.setSchemeid(DNET_PROVENANCE_ACTIONS);
		provenanceAction.setSchemename(DNET_PROVENANCE_ACTIONS);
		info.setProvenanceaction(provenanceAction);

		// TODO calculate the trust value based on the similarity score of the elements in the CC
		// info.setTrust();

		r.setDataInfo(info);
		return r;
	}

	public String createDedupID(String id) {

		String prefix = id.split("\\|")[0];
		return prefix + "|dedup_wf_001::" + DHPUtils.md5(id);
	}
}
