
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
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

public class PrepareSimpleEntitiesJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareSimpleEntitiesJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareSimpleEntitiesJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String workingDir = parser.get("workingDir");
		log.info("workingDir: {}", workingDir);

		final String simpleEntitiesPath = workingDir + "/simpleEntities";
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

		final Encoder<OaBrokerMainEntity> encoder = Encoders.bean(OaBrokerMainEntity.class);
		return ClusterUtils
			.readPath(spark, graphPath + "/" + sourceClass.getSimpleName().toLowerCase(), sourceClass)
			.filter((FilterFunction<SRC>) r -> !ClusterUtils.isDedupRoot(r.getId()))
			.filter((FilterFunction<SRC>) r -> r.getDataInfo().getDeletedbyinference())
			.map((MapFunction<SRC, OaBrokerMainEntity>) ConversionUtils::oafResultToBrokerResult, encoder);
	}

}
