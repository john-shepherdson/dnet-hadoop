
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class SparkCopyOpenorgs extends AbstractSparkAction {
	private static final Logger log = LoggerFactory.getLogger(SparkCopyOpenorgs.class);

	public SparkCopyOpenorgs(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgs_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkCopyOpenorgs(parser, getSparkSession(conf))
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

		String subEntity = "organization";
		log.info("Copying openorgs to the working dir");

		final String outputPath = DedupUtility.createDedupRecordPath(workingPath, actionSetId, subEntity);
		removeOutputDir(spark, outputPath);

		final String entityPath = DedupUtility.createEntityPath(graphBasePath, subEntity);

		final Class<OafEntity> clazz = ModelSupport.entityTypes.get(EntityType.valueOf(subEntity));

		filterEntities(spark, entityPath, clazz)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

	}

	public static <T extends OafEntity> Dataset<T> filterEntities(
		final SparkSession spark,
		final String entitiesInputPath,
		final Class<T> clazz) {

		// <id, json_entity>
		Dataset<T> entities = spark
			.read()
			.textFile(entitiesInputPath)
			.map(
				(MapFunction<String, T>) it -> {
					T entity = OBJECT_MAPPER.readValue(it, clazz);
					return entity;
				},
				Encoders.kryo(clazz));

		return entities.filter(entities.col("id").contains("openorgs____"));
	}

}
