
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
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
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
				PrepareRelationsJob.class
					.getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_related_entities_pahase1.json"));
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

		final Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		final SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ProvisionModelSupport.getModelClasses());

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
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Relation.class)))
			.cache();

		readPathEntity(spark, inputEntityPath, clazz)
			.filter("dataInfo.invisible == false")
			.map(
				(MapFunction<E, Tuple2<String, RelatedEntity>>) e -> new Tuple2<>(e.getId(), asRelatedEntity(e, clazz)),
				Encoders
					.tuple(Encoders.STRING(), Encoders.kryo(RelatedEntity.class)))
			.write()
			.mode(SaveMode.Overwrite)
			.save("/tmp/beta_provision/working_dir/update_solr/join_partial/relatedEntities/" + clazz.getSimpleName());

		final Dataset<Tuple2<String, RelatedEntity>> entities = spark
			.read()
			.load("/tmp/beta_provision/working_dir/update_solr/join_partial/relatedEntities/" + clazz.getSimpleName())
			.as(
				Encoders
					.tuple(Encoders.STRING(), Encoders.kryo(RelatedEntity.class)));

		relsByTarget
			.joinWith(entities, entities.col("_1").equalTo(relsByTarget.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, RelatedEntity>>, RelatedEntityWrapper>) t -> new RelatedEntityWrapper(
					t._1()._2(), t._2()._2()),
				Encoders.kryo(RelatedEntityWrapper.class))
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
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

		if (entity.getPid() != null)
			re.setPid(entity.getPid().stream().limit(400).collect(Collectors.toList()));
		re.setCollectedfrom(entity.getCollectedfrom());

		switch (EntityType.fromClass(clazz)) {
			case publication:
			case dataset:
			case otherresearchproduct:
			case software:
				final Result result = (Result) entity;

				if (result.getTitle() != null && !result.getTitle().isEmpty()) {
					final StructuredProperty title = result.getTitle().stream().findFirst().get();
					title.setValue(StringUtils.left(title.getValue(), ModelHardLimits.MAX_TITLE_LENGTH));
					re.setTitle(title);
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
					re.setFundingtree(f.stream().map(s -> s.getValue()).collect(Collectors.toList()));
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
			.map(x -> x.getValue())
			.orElse(defaultValue);
	}

	/**
	 * Reads a Dataset of eu.dnetlib.dhp.oa.provision.model.SortableRelation objects from a newline delimited json text file,
	 *
	 * @param spark
	 * @param relationPath
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
