
package eu.dnetlib.dhp.oa.dedup;

import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PROVENANCE_DEDUP;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.graph.ConnectedComponent;
import eu.dnetlib.dhp.oa.dedup.graph.GraphProcessor;
import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class SparkCreateMergeRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateMergeRels.class);

	public SparkCreateMergeRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateMergeRels.class
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
		throws ISLookUpException, DocumentException, IOException, SAXException {

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
			final Class<OafEntity> clazz = ModelSupport.entityTypes.get(EntityType.valueOf(subEntity));

			log.info("Creating merge rels for: '{}'", subEntity);

			final int maxIterations = dedupConf.getWf().getMaxIterations();
			log.info("Max iterations {}", maxIterations);

			final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);

			// <hash(id), id>
			JavaPairRDD<Object, String> vertexes = createVertexes(sc, graphBasePath, subEntity, dedupConf);

			final RDD<Edge<String>> edgeRdd = spark
				.read()
				.load(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity))
				.as(Encoders.bean(Relation.class))
				.javaRDD()
				.map(it -> new Edge<>(hash(it.getSource()), hash(it.getTarget()), it.getRelClass()))
				.rdd();

			Dataset<Tuple2<String, String>> rawMergeRels = spark
				.createDataset(
					GraphProcessor
						.findCCs(vertexes.rdd(), edgeRdd, maxIterations, cut)
						.toJavaRDD()
						.filter(k -> k.getIds().size() > 1)
						.flatMap(this::ccToRels)
						.rdd(),
					Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

			Dataset<Tuple2<String, OafEntity>> entities = spark
				.read()
				.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.map(
					(MapFunction<String, Tuple2<String, OafEntity>>) it -> {
						OafEntity entity = OBJECT_MAPPER.readValue(it, clazz);
						return new Tuple2<>(entity.getId(), entity);
					},
					Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));

			Dataset<Relation> mergeRels = rawMergeRels
				.joinWith(entities, rawMergeRels.col("_2").equalTo(entities.col("_1")), "inner")
				// <tmp_source,target>,<target,entity>
				.map(
					(MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, OafEntity>>, Tuple2<String, OafEntity>>) value -> new Tuple2<>(
						value._1()._1(), value._2()._2()),
					Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)))
				// <tmp_source,entity>
				.groupByKey(
					(MapFunction<Tuple2<String, OafEntity>, String>) Tuple2::_1, Encoders.STRING())
				.mapGroups(
					(MapGroupsFunction<String, Tuple2<String, OafEntity>, ConnectedComponent>) this::generateID,
					Encoders.bean(ConnectedComponent.class))
				// <root_id, list(target)>
				.flatMap(
					(FlatMapFunction<ConnectedComponent, Relation>) cc -> ccToMergeRel(cc, dedupConf),
					Encoders.bean(Relation.class));

			saveParquet(mergeRels, mergeRelPath, SaveMode.Overwrite);

		}
	}

	private <T extends OafEntity> ConnectedComponent generateID(String key, Iterator<Tuple2<String, T>> values) {

		List<Identifier<T>> identifiers = Lists
			.newArrayList(values)
			.stream()
			.map(v -> Identifier.newInstance(v._2()))
			.collect(Collectors.toList());

		String rootID = IdGenerator.generate(identifiers, key);

		if (Objects.equals(rootID, key))
			throw new IllegalStateException("generated default ID: " + rootID);

		return new ConnectedComponent(rootID,
			identifiers.stream().map(i -> i.getEntity().getId()).collect(Collectors.toSet()));
	}

	private JavaPairRDD<Object, String> createVertexes(JavaSparkContext sc, String graphBasePath, String subEntity,
		DedupConfig dedupConf) {

		return sc
			.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
			.mapToPair(json -> {
				String id = MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), json);
				return new Tuple2<>(hash(id), id);
			});
	}

	private Iterator<Tuple2<String, String>> ccToRels(ConnectedComponent cc) {
		return cc
			.getIds()
			.stream()
			.map(id -> new Tuple2<>(cc.getCcId(), id))
			.iterator();
	}

	private Iterator<Relation> ccToMergeRel(ConnectedComponent cc, DedupConfig dedupConf) {
		return cc
			.getIds()
			.stream()
			.flatMap(
				id -> {
					List<Relation> tmp = new ArrayList<>();

					tmp.add(rel(cc.getCcId(), id, ModelConstants.MERGES, dedupConf));
					tmp.add(rel(id, cc.getCcId(), ModelConstants.IS_MERGED_IN, dedupConf));

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
		r.setSubRelType(ModelConstants.DEDUP);

		DataInfo info = new DataInfo();
		info.setDeletedbyinference(false);
		info.setInferred(true);
		info.setInvisible(false);
		info.setInferenceprovenance(dedupConf.getWf().getConfigurationId());
		Qualifier provenanceAction = new Qualifier();
		provenanceAction.setClassid(PROVENANCE_DEDUP);
		provenanceAction.setClassname(PROVENANCE_DEDUP);
		provenanceAction.setSchemeid(DNET_PROVENANCE_ACTIONS);
		provenanceAction.setSchemename(DNET_PROVENANCE_ACTIONS);
		info.setProvenanceaction(provenanceAction);

		// TODO calculate the trust value based on the similarity score of the elements in the CC

		r.setDataInfo(info);
		return r;
	}

	public static long hash(final String id) {
		return Hashing.murmur3_128().hashString(id).asLong();
	}
}
