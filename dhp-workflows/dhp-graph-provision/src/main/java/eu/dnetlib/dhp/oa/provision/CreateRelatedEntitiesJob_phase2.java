
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.model.ProvisionModelSupport;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntityWrapper;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Joins the graph nodes by resolving the links of distance = 1 to create an adjacency list of linked objects. The
 * operation considers all the entity types (publication, dataset, software, ORP, project, datasource, organization, and
 * all the possible relationships (similarity links produced by the Dedup process are excluded).
 * <p>
 * The operation is implemented by sequentially joining one entity type at time (E) with the relationships (R), and
 * again by E, finally grouped by E.id;
 * <p>
 * The workflow is organized in different parts aimed to to reduce the complexity of the operation 1)
 * PrepareRelationsJob: only consider relationships that are not virtually deleted ($.dataInfo.deletedbyinference ==
 * false), each entity can be linked at most to 100 other objects
 * <p>
 * 2) JoinRelationEntityByTargetJob: (phase 1): prepare tuples [relation - target entity] (R - T): for each entity type
 * E_i map E_i as RelatedEntity T_i to simplify the model and extracting only the necessary information join (R.target =
 * T_i.id) save the tuples (R_i, T_i) (phase 2): create the union of all the entity types E, hash by id read the tuples
 * (R, T), hash by R.source join E.id = (R, T).source, where E becomes the Source Entity S save the tuples (S, R, T)
 * <p>
 * 3) AdjacencyListBuilderJob: given the tuple (S - R - T) we need to group by S.id -> List [ R - T ], mapping the
 * result as JoinedEntity
 * <p>
 * 4) XmlConverterJob: convert the JoinedEntities as XML records
 */
public class CreateRelatedEntitiesJob_phase2 {

	private static final Logger log = LoggerFactory.getLogger(CreateRelatedEntitiesJob_phase2.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareRelationsJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/provision/input_params_related_entities_pahase2.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputRelatedEntitiesPath = parser.get("inputRelatedEntitiesPath");
		log.info("inputRelatedEntitiesPath: {}", inputRelatedEntitiesPath);

		String inputEntityPath = parser.get("inputEntityPath");
		log.info("inputEntityPath: {}", inputEntityPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		int numPartitions = Integer.parseInt(parser.get("numPartitions"));
		log.info("numPartitions: {}", numPartitions);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ProvisionModelSupport.getModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				joinEntityWithRelatedEntities(
					spark, inputRelatedEntitiesPath, inputEntityPath, outputPath, numPartitions, entityClazz);
			});
	}

	private static <E extends OafEntity> void joinEntityWithRelatedEntities(
		SparkSession spark,
		String relatedEntitiesPath,
		String entityPath,
		String outputPath,
		int numPartitions,
		Class<E> entityClazz) {

		Dataset<Tuple2<String, E>> entities = readPathEntity(spark, entityPath, entityClazz);
		Dataset<Tuple2<String, RelatedEntityWrapper>> relatedEntities = readRelatedEntities(
			spark, relatedEntitiesPath, entityClazz);

		TypedColumn<JoinedEntity, JoinedEntity> aggregator = new AdjacencyListAggregator().toColumn();

		entities
			.joinWith(relatedEntities, entities.col("_1").equalTo(relatedEntities.col("_1")), "left_outer")
			.map((MapFunction<Tuple2<Tuple2<String, E>, Tuple2<String, RelatedEntityWrapper>>, JoinedEntity>) value -> {
				JoinedEntity je = new JoinedEntity(value._1()._2());
				Optional
					.ofNullable(value._2())
					.map(Tuple2::_2)
					.ifPresent(r -> je.getLinks().add(r));
				return je;
			}, Encoders.kryo(JoinedEntity.class))
			.filter(filterEmptyEntityFn())
			.groupByKey(
				(MapFunction<JoinedEntity, String>) value -> value.getEntity().getId(),
				Encoders.STRING())
			.agg(aggregator)
			.map(
				(MapFunction<Tuple2<String, JoinedEntity>, JoinedEntity>) value -> value._2(),
				Encoders.kryo(JoinedEntity.class))
			.filter(filterEmptyEntityFn())
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	public static class AdjacencyListAggregator extends Aggregator<JoinedEntity, JoinedEntity, JoinedEntity> {

		@Override
		public JoinedEntity zero() {
			return new JoinedEntity();
		}

		@Override
		public JoinedEntity reduce(JoinedEntity b, JoinedEntity a) {
			return mergeAndGet(b, a);
		}

		private JoinedEntity mergeAndGet(JoinedEntity b, JoinedEntity a) {
			b
				.setEntity(
					Optional
						.ofNullable(a.getEntity())
						.orElse(
							Optional
								.ofNullable(b.getEntity())
								.orElse(null)));
			b.getLinks().addAll(a.getLinks());
			return b;
		}

		@Override
		public JoinedEntity merge(JoinedEntity b, JoinedEntity a) {
			return mergeAndGet(b, a);
		}

		@Override
		public JoinedEntity finish(JoinedEntity j) {
			return j;
		}

		@Override
		public Encoder<JoinedEntity> bufferEncoder() {
			return Encoders.kryo(JoinedEntity.class);
		}

		@Override
		public Encoder<JoinedEntity> outputEncoder() {
			return Encoders.kryo(JoinedEntity.class);
		}

	}

	private static <E extends OafEntity> Dataset<Tuple2<String, RelatedEntityWrapper>> readRelatedEntities(
		SparkSession spark, String inputRelatedEntitiesPath, Class<E> entityClazz) {

		log.info("Reading related entities from: {}", inputRelatedEntitiesPath);

		final List<String> paths = HdfsSupport
			.listFiles(inputRelatedEntitiesPath, spark.sparkContext().hadoopConfiguration());

		log.info("Found paths: {}", String.join(",", paths));

		final String idPrefix = ModelSupport.getIdPrefix(entityClazz);

		return spark
			.read()
			.load(toSeq(paths))
			.as(Encoders.kryo(RelatedEntityWrapper.class))
			.filter((FilterFunction<RelatedEntityWrapper>) e -> e.getRelation().getSource().startsWith(idPrefix))
			.map(
				(MapFunction<RelatedEntityWrapper, Tuple2<String, RelatedEntityWrapper>>) value -> new Tuple2<>(
					value.getRelation().getSource(), value),
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(RelatedEntityWrapper.class)));
	}

	private static <E extends OafEntity> Dataset<Tuple2<String, E>> readPathEntity(
		SparkSession spark, String inputEntityPath, Class<E> entityClazz) {

		log.info("Reading Graph table from: {}", inputEntityPath);
		return spark
			.read()
			.textFile(inputEntityPath)
			.map(
				(MapFunction<String, E>) value -> OBJECT_MAPPER.readValue(value, entityClazz),
				Encoders.bean(entityClazz))
			.filter("dataInfo.invisible == false")
			.map((MapFunction<E, E>) e -> pruneOutliers(entityClazz, e), Encoders.bean(entityClazz))
			.map(
				(MapFunction<E, Tuple2<String, E>>) e -> new Tuple2<>(e.getId(), e),
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(entityClazz)));
	}

	private static <E extends OafEntity> E pruneOutliers(Class<E> entityClazz, E e) {
		if (ModelSupport.isSubClass(entityClazz, Result.class)) {
			Result r = (Result) e;
			if (r.getExternalReference() != null) {
				List<ExternalReference> refs = r
					.getExternalReference()
					.stream()
					.limit(ModelHardLimits.MAX_EXTERNAL_ENTITIES)
					.collect(Collectors.toList());
				r.setExternalReference(refs);
			}
			if (r.getAuthor() != null) {
				List<Author> authors = Lists.newArrayList();
				for (Author a : r.getAuthor()) {
					a.setFullname(StringUtils.left(a.getFullname(), ModelHardLimits.MAX_AUTHOR_FULLNAME_LENGTH));
					if (authors.size() < ModelHardLimits.MAX_AUTHORS || hasORCID(a)) {
						authors.add(a);
					}
				}
				r.setAuthor(authors);
			}
			if (r.getDescription() != null) {
				List<Field<String>> desc = r
					.getDescription()
					.stream()
					.filter(Objects::nonNull)
					.map(d -> {
						d.setValue(StringUtils.left(d.getValue(), ModelHardLimits.MAX_ABSTRACT_LENGTH));
						return d;
					})
					.collect(Collectors.toList());
				r.setDescription(desc);
			}
			if (r.getTitle() != null) {
				List<StructuredProperty> titles = r
					.getTitle()
					.stream()
					.filter(Objects::nonNull)
					.map(t -> {
						t.setValue(StringUtils.left(t.getValue(), ModelHardLimits.MAX_TITLE_LENGTH));
						return t;
					})
					.limit(ModelHardLimits.MAX_TITLES)
					.collect(Collectors.toList());
				r.setTitle(titles);
			}
		}
		return e;
	}

	private static boolean hasORCID(Author a) {
		return a.getPid() != null && a
			.getPid()
			.stream()
			.filter(Objects::nonNull)
			.map(StructuredProperty::getQualifier)
			.filter(Objects::nonNull)
			.map(Qualifier::getClassid)
			.filter(StringUtils::isNotBlank)
			.anyMatch(c -> "orcid".equals(c.toLowerCase()));
	}

	private static FilterFunction<JoinedEntity> filterEmptyEntityFn() {
		return (FilterFunction<JoinedEntity>) v -> Objects.nonNull(v.getEntity());
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static Seq<String> toSeq(List<String> list) {
		return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
	}
}
