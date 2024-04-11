
package eu.dnetlib.dhp.oa.merge;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

/**
 * Groups the graph content by entity identifier to ensure ID uniqueness
 */
public class GroupEntitiesSparkJob {
	private static final Logger log = LoggerFactory.getLogger(GroupEntitiesSparkJob.class);

	private static final Encoder<OafEntity> OAFENTITY_KRYO_ENC = Encoders.kryo(OafEntity.class);

	private ArgumentApplicationParser parser;

	public GroupEntitiesSparkJob(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				GroupEntitiesSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/merge/group_graph_entities_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);

		new GroupEntitiesSparkJob(parser).run(isSparkSessionManaged, isLookupService);
	}

	public void run(Boolean isSparkSessionManaged, ISLookUpService isLookUpService)
		throws ISLookUpException {

		String graphInputPath = parser.get("graphInputPath");
		log.info("graphInputPath: {}", graphInputPath);

		String checkpointPath = parser.get("checkpointPath");
		log.info("checkpointPath: {}", checkpointPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		boolean filterInvisible = Boolean.parseBoolean(parser.get("filterInvisible"));
		log.info("filterInvisible: {}", filterInvisible);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		final VocabularyGroup vocs = VocabularyGroup.loadVocsFromIS(isLookUpService);

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(checkpointPath, spark.sparkContext().hadoopConfiguration());
				groupEntities(spark, graphInputPath, checkpointPath, outputPath, filterInvisible, vocs);
			});
	}

	private static void groupEntities(
		SparkSession spark,
		String inputPath,
		String checkpointPath,
		String outputPath,
		boolean filterInvisible, VocabularyGroup vocs) {

		Dataset<OafEntity> allEntities = spark.emptyDataset(OAFENTITY_KRYO_ENC);

		for (Map.Entry<EntityType, Class> e : ModelSupport.entityTypes.entrySet()) {
			String entity = e.getKey().name();
			Class<? extends OafEntity> entityClass = e.getValue();
			String entityInputPath = inputPath + "/" + entity;

			if (!HdfsSupport.exists(entityInputPath, spark.sparkContext().hadoopConfiguration())) {
				continue;
			}

			allEntities = allEntities
				.union(
					((Dataset<OafEntity>) spark
						.read()
						.schema(Encoders.bean(entityClass).schema())
						.json(entityInputPath)
						.filter("length(id) > 0")
						.as(Encoders.bean(entityClass)))
							.map((MapFunction<OafEntity, OafEntity>) r -> r, OAFENTITY_KRYO_ENC));
		}

		Dataset<?> groupedEntities = allEntities
			.map(
				(MapFunction<OafEntity, OafEntity>) entity -> GraphCleaningFunctions
					.applyCoarVocabularies(entity, vocs),
				OAFENTITY_KRYO_ENC)
			.groupByKey((MapFunction<OafEntity, String>) OafEntity::getId, Encoders.STRING())
			.reduceGroups((ReduceFunction<OafEntity>) MergeUtils::checkedMerge)
			.map(
				(MapFunction<Tuple2<String, OafEntity>, Tuple2<String, OafEntity>>) t -> new Tuple2<>(
					t._2().getClass().getName(), t._2()),
				Encoders.tuple(Encoders.STRING(), OAFENTITY_KRYO_ENC));

		// pivot on "_1" (classname of the entity)
		// created columns containing only entities of the same class
		for (Map.Entry<EntityType, Class> e : ModelSupport.entityTypes.entrySet()) {
			String entity = e.getKey().name();
			Class<? extends OafEntity> entityClass = e.getValue();

			groupedEntities = groupedEntities
				.withColumn(
					entity,
					when(col("_1").equalTo(entityClass.getName()), col("_2")));
		}

		groupedEntities
			.drop("_1", "_2")
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.save(checkpointPath);

		ForkJoinPool parPool = new ForkJoinPool(ModelSupport.entityTypes.size());

		ModelSupport.entityTypes
			.entrySet()
			.stream()
			.map(e -> parPool.submit(() -> {
				String entity = e.getKey().name();
				Class<? extends OafEntity> entityClass = e.getValue();

				spark
					.read()
					.load(checkpointPath)
					.select(col(entity).as("value"))
					.filter("value IS NOT NULL")
					.as(OAFENTITY_KRYO_ENC)
					.map((MapFunction<OafEntity, OafEntity>) r -> r, (Encoder<OafEntity>) Encoders.bean(entityClass))
					.filter(filterInvisible ? "dataInfo.invisible != TRUE" : "TRUE")
					.write()
					.mode(SaveMode.Overwrite)
					.option("compression", "gzip")
					.json(outputPath + "/" + entity);
			}))
			.collect(Collectors.toList())
			.forEach(t -> {
				try {
					t.get();
				} catch (InterruptedException | ExecutionException e) {
					throw new RuntimeException(e);
				}
			});
	}
}
