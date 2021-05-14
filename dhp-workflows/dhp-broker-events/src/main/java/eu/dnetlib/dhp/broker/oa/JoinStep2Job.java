
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftware;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftwareAggregator;
import scala.Tuple2;

public class JoinStep2Job {

	private static final Logger log = LoggerFactory.getLogger(JoinStep2Job.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					JoinStep2Job.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String workingDir = parser.get("workingDir");
		log.info("workingDir: {}", workingDir);

		final String joinedEntitiesPath = workingDir + "/joinedEntities_step2";
		log.info("joinedEntitiesPath: {}", joinedEntitiesPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, joinedEntitiesPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_entities");

			final Dataset<OaBrokerMainEntity> sources = ClusterUtils
				.readPath(spark, workingDir + "/joinedEntities_step1", OaBrokerMainEntity.class);

			final Dataset<RelatedSoftware> typedRels = ClusterUtils
				.readPath(spark, workingDir + "/relatedSoftwares", RelatedSoftware.class);

			final TypedColumn<Tuple2<OaBrokerMainEntity, RelatedSoftware>, OaBrokerMainEntity> aggr = new RelatedSoftwareAggregator()
				.toColumn();

			final Dataset<OaBrokerMainEntity> dataset = sources
				.joinWith(typedRels, sources.col("openaireId").equalTo(typedRels.col("source")), "left_outer")
				.groupByKey(
					(MapFunction<Tuple2<OaBrokerMainEntity, RelatedSoftware>, String>) t -> t._1.getOpenaireId(),
					Encoders.STRING())
				.agg(aggr)
				.map(
					(MapFunction<Tuple2<String, OaBrokerMainEntity>, OaBrokerMainEntity>) t -> t._2,
					Encoders.bean(OaBrokerMainEntity.class));

			ClusterUtils.save(dataset, joinedEntitiesPath, OaBrokerMainEntity.class, total);

		});

	}

}
