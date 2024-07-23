
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.ProvisionModelSupport;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntity;
import eu.dnetlib.dhp.oa.provision.model.RelatedEntityWrapper;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.ModelHardLimits;
import scala.Tuple2;

/**
 * CreateRelatedEntitiesJob: (phase 1): prepare tuples [relation - target entity] (R - T): for each entity type E_i map E_i as RelatedEntity
 * T_i to simplify the model and extracting only the necessary information join (R.target = T_i.id) save the tuples (R_i, T_i)
 */
public class CreateRelatedEntitiesJob_phase1 {

	private static final Logger log = LoggerFactory.getLogger(CreateRelatedEntitiesJob_phase1.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {

		final String jsonConfiguration = IOUtils
			.toString(
				Objects
					.requireNonNull(
						CreateRelatedEntitiesJob_phase1.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/oa/provision/input_params_related_entities_pahase1.json")));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputRelationsPath = parser.get("inputRelationsPath");
		log.info("inputRelationsPath: {}", inputRelationsPath);

		final String inputEntityPath = parser.get("inputEntityPath");
		log.info("inputEntityPath: {}", inputEntityPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		@SuppressWarnings("unchecked")
		final Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			removeOutputDir(spark, outputPath);
			joinRelationEntity(spark, inputRelationsPath, inputEntityPath, entityClazz, outputPath);
		});
	}

	private static <E extends OafEntity> void joinRelationEntity(
		final SparkSession spark,
		final String inputRelationsPath,
		final String inputEntityPath,
		final Class<E> clazz,
		final String outputPath) {

		final Dataset<Tuple2<String, Relation>> relsByTarget = readPathRelation(spark, inputRelationsPath)
			.map(
				(MapFunction<Relation, Tuple2<String, Relation>>) r -> new Tuple2<>(r.getTarget(),
					r),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(Relation.class)))
			.cache();

		final Dataset<Tuple2<String, RelatedEntity>> entities = readPathEntity(spark, inputEntityPath, clazz)
			.filter("dataInfo.invisible == false")
			.map(
				(MapFunction<E, Tuple2<String, RelatedEntity>>) e -> new Tuple2<>(e.getId(), asRelatedEntity(e, clazz)),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(RelatedEntity.class)))
			.cache();

		relsByTarget
			.joinWith(entities, entities.col("_1").equalTo(relsByTarget.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, RelatedEntity>>, RelatedEntityWrapper>) t -> new RelatedEntityWrapper(
					t._1()._2(), t._2()._2()),
				Encoders.bean(RelatedEntityWrapper.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static <E extends OafEntity> Dataset<E> readPathEntity(
		final SparkSession spark,
		final String inputEntityPath,
		final Class<E> entityClazz) {

		log.info("Reading Graph table from: {}", inputEntityPath);
		return spark
			.read()
			.textFile(inputEntityPath)
			.map(
				(MapFunction<String, E>) value -> OBJECT_MAPPER.readValue(value, entityClazz),
				Encoders.bean(entityClazz));
	}

	public static <E extends OafEntity> RelatedEntity asRelatedEntity(final E entity, final Class<E> clazz) {

		final RelatedEntity re = new RelatedEntity();
		re.setId(entity.getId());
		re.setType(EntityType.fromClass(clazz).name());

		// TODO move the max number of PIDs to eu.dnetlib.dhp.schema.oaf.utils.ModelHardLimits
		if (Objects.nonNull(entity.getPid())) {
			re.setPid(entity.getPid().stream().limit(400).collect(Collectors.toList()));
		}
		re.setCollectedfrom(entity.getCollectedfrom());

		switch (EntityType.fromClass(clazz)) {
			case publication:
			case dataset:
			case otherresearchproduct:
			case software:
				final Result result = (Result) entity;

				if (Objects.nonNull(result.getTitle()) && !result.getTitle().isEmpty()) {
					result
						.getTitle()
						.stream()
						.filter(t -> StringUtils.isNotBlank(t.getValue()))
						.findFirst()
						.ifPresent(
							title -> {
								re.setTitle(title);
								re
									.getTitle()
									.setValue(StringUtils.left(title.getValue(), ModelHardLimits.MAX_TITLE_LENGTH));
							});
				}
				if (Objects.nonNull(result.getDescription()) && !result.getDescription().isEmpty()) {
					result
						.getDescription()
						.stream()
						.findFirst()
						.map(Field::getValue)
						.ifPresent(
							d -> re.setDescription(StringUtils.left(d, ModelHardLimits.MAX_RELATED_ABSTRACT_LENGTH)));
				}
				if (Objects.nonNull(result.getAuthor()) && !result.getAuthor().isEmpty()) {
					re
						.setAuthor(
							result
								.getAuthor()
								.stream()
								.map(Author::getFullname)
								.filter(StringUtils::isNotBlank)
								.collect(Collectors.toList()));
				}

				re.setDateofacceptance(getValue(result.getDateofacceptance()));
				re.setPublisher(getValue(result.getPublisher()));
				re.setResulttype(result.getResulttype());
				if (Objects.nonNull(result.getInstance())) {
					re
						.setInstances(
							result
								.getInstance()
								.stream()
								.filter(Objects::nonNull)
								.limit(ModelHardLimits.MAX_INSTANCES)
								.collect(Collectors.toList()));
				}

				// TODO still to be mapped
				// re.setCodeRepositoryUrl(j.read("$.coderepositoryurl"));

				break;
			case datasource:
				final Datasource d = (Datasource) entity;

				re.setOfficialname(getValue(d.getOfficialname()));
				re.setWebsiteurl(getValue(d.getWebsiteurl()));
				re.setDatasourcetype(d.getDatasourcetype());
				re.setDatasourcetypeui(d.getDatasourcetypeui());
				re.setOpenairecompatibility(d.getOpenairecompatibility());

				break;
			case organization:
				final Organization o = (Organization) entity;

				re.setLegalname(getValue(o.getLegalname()));
				re.setLegalshortname(getValue(o.getLegalshortname()));
				re.setCountry(o.getCountry());
				re.setWebsiteurl(getValue(o.getWebsiteurl()));
				break;
			case project:
				final Project p = (Project) entity;

				re.setProjectTitle(getValue(p.getTitle()));
				re.setCode(getValue(p.getCode()));
				re.setAcronym(getValue(p.getAcronym()));
				re.setContracttype(p.getContracttype());

				final List<Field<String>> f = p.getFundingtree();
				if (!f.isEmpty()) {
					re.setFundingtree(f.stream().map(Field::getValue).collect(Collectors.toList()));
				}
				break;
		}
		return re;
	}

	private static String getValue(final Field<String> field) {
		return getFieldValueWithDefault(field, "");
	}

	private static <T> T getFieldValueWithDefault(final Field<T> f, final T defaultValue) {
		return Optional
			.ofNullable(f)
			.filter(Objects::nonNull)
			.map(Field::getValue)
			.orElse(defaultValue);
	}

	/**
	 * Reads a Dataset of eu.dnetlib.dhp.oa.provision.model.SortableRelation objects from a newline delimited json text
	 * file
	 *
	 * @param spark the SparkSession
	 * @param relationPath the path storing the relation objects
	 * @return the Dataset<SortableRelation> containing all the relationships
	 */
	private static Dataset<Relation> readPathRelation(
		final SparkSession spark,
		final String relationPath) {

		log.info("Reading relations from: {}", relationPath);
		return spark.read().load(relationPath).as(Encoders.bean(Relation.class));
	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}
}
