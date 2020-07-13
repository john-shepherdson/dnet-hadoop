
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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

import com.google.common.hash.Hashing;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.graph.ConnectedComponent;
import eu.dnetlib.dhp.oa.dedup.graph.GraphProcessor;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class SparkCreateMergeRels extends AbstractSparkAction {

	public static final String PROVENANCE_ACTION_CLASS = "sysimport:dedup";
	private static final Logger log = LoggerFactory.getLogger(SparkCreateMergeRels.class);
	public static final String DNET_PROVENANCE_ACTIONS = "dnet:provenanceActions";

	public SparkCreateMergeRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));
		parser.parseArgument(args);

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookupUrl {}", isLookUpUrl);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkCreateMergeRels(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(isLookUpUrl));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws ISLookUpException, DocumentException, IOException {

		final String graphBasePath = parser.get("graphBasePath");
		final String workingPath = parser.get("workingPath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		int cut = Optional
			.ofNullable(parser.get("cutConnectedComponent"))
			.map(Integer::valueOf)
			.orElse(0);
		log.info("connected component cut: '{}'", cut);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {
			final String subEntity = dedupConf.getWf().getSubEntityValue();

			log.info("Creating mergerels for: '{}'", subEntity);

			final int maxIterations = dedupConf.getWf().getMaxIterations();
			log.info("Max iterations {}", maxIterations);

			final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);

			final JavaPairRDD<Object, String> vertexes = sc
				.textFile(graphBasePath + "/" + subEntity)
				.map(s -> MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), s))
				.mapToPair((PairFunction<String, Object, String>) s -> new Tuple2<>(hash(s), s));

			final RDD<Edge<String>> edgeRdd = spark
				.read()
				.textFile(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity))
				.map(
					(MapFunction<String, Relation>) r -> OBJECT_MAPPER.readValue(r, Relation.class),
					Encoders.bean(Relation.class))
				.javaRDD()
				.map(it -> new Edge<>(hash(it.getSource()), hash(it.getTarget()), it.getRelClass()))
				.rdd();

			final Dataset<Relation> mergeRels = spark
				.createDataset(
					GraphProcessor
						.findCCs(vertexes.rdd(), edgeRdd, maxIterations, cut)
						.toJavaRDD()
						.filter(k -> k.getDocIds().size() > 1)
						.flatMap(cc -> ccToMergeRel(cc, dedupConf))
						.rdd(),
					Encoders.bean(Relation.class));

			mergeRels.write().mode(SaveMode.Append).parquet(mergeRelPath);

		}
	}

	public Iterator<Relation> ccToMergeRel(ConnectedComponent cc, DedupConfig dedupConf) {
		return cc
			.getDocIds()
			.stream()
			.flatMap(
				id -> {
					List<Relation> tmp = new ArrayList<>();

					tmp.add(rel(cc.getCcId(), id, "merges", dedupConf));
					tmp.add(rel(id, cc.getCcId(), "isMergedIn", dedupConf));

					return tmp.stream();
				})
			.iterator();
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

	public static long hash(final String id) {
		return Hashing.murmur3_128().hashString(id).asLong();
	}
}
