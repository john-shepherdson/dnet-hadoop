
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;

public class PrepareSimpleEntititiesJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareSimpleEntititiesJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareSimpleEntititiesJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		final String simpleEntitiesPath = workingPath + "/simpleEntities";
		log.info("simpleEntitiesPath: {}", simpleEntitiesPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, simpleEntitiesPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_entities");

			final Dataset<OaBrokerMainEntity> dataset = prepareSimpleEntities(spark, graphPath, Publication.class)
				.union(prepareSimpleEntities(spark, graphPath, eu.dnetlib.dhp.schema.oaf.Dataset.class))
				.union(prepareSimpleEntities(spark, graphPath, Software.class))
				.union(prepareSimpleEntities(spark, graphPath, OtherResearchProduct.class));

			ClusterUtils.save(dataset, simpleEntitiesPath, OaBrokerMainEntity.class, total);
		});

	}

	private static <SRC extends Result> Dataset<OaBrokerMainEntity> prepareSimpleEntities(
		final SparkSession spark,
		final String graphPath,
		final Class<SRC> sourceClass) {

		return ClusterUtils
			.readPath(spark, graphPath + "/" + sourceClass.getSimpleName().toLowerCase(), sourceClass)
			.filter(r -> !ClusterUtils.isDedupRoot(r.getId()))
			.filter(r -> r.getDataInfo().getDeletedbyinference())
			.map(ConversionUtils::oafResultToBrokerResult, Encoders.bean(OaBrokerMainEntity.class));
	}

}
