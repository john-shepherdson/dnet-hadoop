
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;

public class GenerateSimpleEntitities {

	private static final Logger log = LoggerFactory.getLogger(GenerateSimpleEntitities.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateSimpleEntitities.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/generate_simple_entities.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String simpleEntitiesPath = parser.get("simpleEntitiesPath");
		log.info("simpleEntitiesPath: {}", simpleEntitiesPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, simpleEntitiesPath);

			expandResultsWithRelations(spark, graphPath, Publication.class)
				.write()
				.mode(SaveMode.Overwrite)
				.json(simpleEntitiesPath);

			// TODO UNCOMMENT THIS
			// spark
			// .emptyDataset(Encoders.bean(Event.class))
			// .union(generateEvents(spark, graphPath, Publication.class, dedupConfig))
			// .union(generateEvents(spark, graphPath, eu.dnetlib.dhp.schema.oaf.Dataset.class, dedupConfig))
			// .union(generateEvents(spark, graphPath, Software.class, dedupConfig))
			// .union(generateEvents(spark, graphPath, OtherResearchProduct.class, dedupConfig))
			// .write()
			// .mode(SaveMode.Overwrite)
			// .option("compression", "gzip")
			// .json(eventsPath);
		});

	}

	private static <SRC extends Result> Dataset<OaBrokerMainEntity> expandResultsWithRelations(
		final SparkSession spark,
		final String graphPath,
		final Class<SRC> sourceClass) {

		return ClusterUtils
			.readPath(spark, graphPath + "/" + sourceClass.getSimpleName().toLowerCase(), sourceClass)
			.filter(r -> r.getDataInfo().getDeletedbyinference())
			.map(ConversionUtils::oafResultToBrokerResult, Encoders.bean(OaBrokerMainEntity.class));
	}

}
