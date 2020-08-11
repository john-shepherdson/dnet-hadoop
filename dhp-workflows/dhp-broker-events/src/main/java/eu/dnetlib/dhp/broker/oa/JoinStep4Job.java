
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
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublication;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublicationAggregator;
import scala.Tuple2;

public class JoinStep4Job {

	private static final Logger log = LoggerFactory.getLogger(JoinStep4Job.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					JoinStep4Job.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		final String joinedEntitiesPath = workingPath + "/joinedEntities_step4";
		log.info("joinedEntitiesPath: {}", joinedEntitiesPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, joinedEntitiesPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_entities");

			final Dataset<OaBrokerMainEntity> sources = ClusterUtils
				.readPath(spark, workingPath + "/joinedEntities_step3", OaBrokerMainEntity.class);

			final Dataset<RelatedPublication> typedRels = ClusterUtils
				.readPath(spark, workingPath + "/relatedPublications", RelatedPublication.class);

			final TypedColumn<Tuple2<OaBrokerMainEntity, RelatedPublication>, OaBrokerMainEntity> aggr = new RelatedPublicationAggregator()
				.toColumn();

			final Dataset<OaBrokerMainEntity> dataset = sources
				.joinWith(typedRels, sources.col("openaireId").equalTo(typedRels.col("source")), "left_outer")
				.groupByKey(
					(MapFunction<Tuple2<OaBrokerMainEntity, RelatedPublication>, String>) t -> t._1.getOpenaireId(),
					Encoders.STRING())
				.agg(aggr)
				.map(t -> t._2, Encoders.bean(OaBrokerMainEntity.class));

			ClusterUtils.save(dataset, joinedEntitiesPath, OaBrokerMainEntity.class, total);

		});

	}

}
