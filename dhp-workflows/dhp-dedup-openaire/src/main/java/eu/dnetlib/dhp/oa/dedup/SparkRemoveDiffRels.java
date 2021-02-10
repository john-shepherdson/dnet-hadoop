
package eu.dnetlib.dhp.oa.dedup;

import static eu.dnetlib.dhp.oa.dedup.SparkCreateMergeRels.DNET_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.oa.dedup.SparkCreateMergeRels.PROVENANCE_ACTION_CLASS;
import static eu.dnetlib.dhp.oa.dedup.SparkCreateMergeRels.hash;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.graph.ConnectedComponent;
import eu.dnetlib.dhp.oa.dedup.graph.GraphProcessor;
import eu.dnetlib.dhp.oa.dedup.model.Block;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class SparkRemoveDiffRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkRemoveDiffRels.class);

	public SparkRemoveDiffRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkCreateSimRels(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws DocumentException, IOException, ISLookUpException {

		// read oozie parameters
		final String graphBasePath = parser.get("graphBasePath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");
		final int numPartitions = Optional
			.ofNullable(parser.get("numPartitions"))
			.map(Integer::valueOf)
			.orElse(NUM_PARTITIONS);

		log.info("numPartitions: '{}'", numPartitions);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		// for each dedup configuration
		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {

			final String entity = dedupConf.getWf().getEntityType();
			final String subEntity = dedupConf.getWf().getSubEntityValue();
			log.info("Removing diffrels for: '{}'", subEntity);

			final String mergeRelsPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);

			final String relationPath = DedupUtility.createEntityPath(graphBasePath, subEntity);

			final int maxIterations = dedupConf.getWf().getMaxIterations();
			log.info("Max iterations {}", maxIterations);

			JavaRDD<Relation> mergeRelsRDD = spark
				.read()
				.load(mergeRelsPath)
				.as(Encoders.bean(Relation.class))
				.where("relClass == 'merges'")
				.toJavaRDD();

			JavaRDD<Tuple2<Tuple2<String, String>, String>> diffRelsRDD = spark
				.read()
				.textFile(relationPath)
				.map(patchRelFn(), Encoders.bean(Relation.class))
				.toJavaRDD()
				.filter(r -> filterRels(r, entity))
				.map(rel -> {
					if (rel.getSource().compareTo(rel.getTarget()) < 0)
						return new Tuple2<>(new Tuple2<>(rel.getSource(), rel.getTarget()), "diffRel");
					else
						return new Tuple2<>(new Tuple2<>(rel.getTarget(), rel.getSource()), "diffRel");
				});

			JavaRDD<Tuple2<Tuple2<String, String>, String>> flatMergeRels = mergeRelsRDD
				.mapToPair(rel -> new Tuple2<>(rel.getSource(), rel.getTarget()))
				.groupByKey()
				.flatMap(g -> {
					List<Tuple2<Tuple2<String, String>, String>> rels = new ArrayList<>();

					List<String> ids = StreamSupport
						.stream(g._2().spliterator(), false)
						.collect(Collectors.toList());

					for (int i = 0; i < ids.size(); i++) {
						for (int j = i + 1; j < ids.size(); j++) {
							if (ids.get(i).compareTo(ids.get(j)) < 0)
								rels.add(new Tuple2<>(new Tuple2<>(ids.get(i), ids.get(j)), g._1()));
							else
								rels.add(new Tuple2<>(new Tuple2<>(ids.get(j), ids.get(i)), g._1()));
						}
					}
					return rels.iterator();

				});

			JavaRDD<Relation> purgedMergeRels = flatMergeRels
				.union(diffRelsRDD)
				.mapToPair(rel -> new Tuple2<>(rel._1(), Arrays.asList(rel._2())))
				.reduceByKey((a, b) -> {
					List<String> list = new ArrayList<String>();
					list.addAll(a);
					list.addAll(b);
					return list;
				})
				.filter(rel -> rel._2().size() == 1)
				.mapToPair(rel -> new Tuple2<>(rel._2().get(0), rel._1()))
				.flatMap(rel -> {
					List<Tuple2<String, String>> rels = new ArrayList<>();
					String source = rel._1();
					rels.add(new Tuple2<>(source, rel._2()._1()));
					rels.add(new Tuple2<>(source, rel._2()._2()));
					return rels.iterator();
				})
				.distinct()
				.flatMap(rel -> tupleToMergeRel(rel, dedupConf));

			spark
				.createDataset(purgedMergeRels.rdd(), Encoders.bean(Relation.class))
				.write()
				.mode(SaveMode.Overwrite)
				.json(mergeRelsPath);
		}
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

	private boolean filterRels(Relation rel, String entityType) {

		switch (entityType) {
			case "result":
				if (rel.getRelClass().equals("isDifferentFrom") && rel.getRelType().equals("resultResult")
					&& rel.getSubRelType().equals("dedup"))
					return true;
				break;
			case "organization":
				if (rel.getRelClass().equals("isDifferentFrom") && rel.getRelType().equals("organizationOrganization")
					&& rel.getSubRelType().equals("dedup"))
					return true;
				break;
			default:
				return false;
		}
		return false;
	}

	public Iterator<Relation> tupleToMergeRel(Tuple2<String, String> rel, DedupConfig dedupConf) {

		List<Relation> rels = new ArrayList<>();

		rels.add(rel(rel._1(), rel._2(), "merges", dedupConf));
		rels.add(rel(rel._2(), rel._1(), "isMergedIn", dedupConf));

		return rels.iterator();
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
}
