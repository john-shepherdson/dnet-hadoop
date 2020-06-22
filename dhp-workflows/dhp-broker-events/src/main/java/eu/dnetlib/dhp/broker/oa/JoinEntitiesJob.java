
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.TypedColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.OaBrokerMainEntityAggregator;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDataset;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedProject;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublication;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftware;
import scala.Tuple2;

public class JoinEntitiesJob {

	private static final Logger log = LoggerFactory.getLogger(JoinEntitiesJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					JoinEntitiesJob.class
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

		final String joinedEntitiesPath = workingPath + "/joinedEntities";
		log.info("joinedEntitiesPath: {}", joinedEntitiesPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, joinedEntitiesPath);

			final Dataset<OaBrokerMainEntity> r0 = ClusterUtils
				.readPath(spark, graphPath + "/simpleEntities", OaBrokerMainEntity.class);

			final Dataset<OaBrokerMainEntity> r1 = join(
				r0, ClusterUtils.readPath(spark, graphPath + "/relatedProjects", RelatedProject.class));
			final Dataset<OaBrokerMainEntity> r2 = join(
				r1, ClusterUtils.readPath(spark, graphPath + "/relatedDatasets", RelatedDataset.class));
			final Dataset<OaBrokerMainEntity> r3 = join(
				r2, ClusterUtils.readPath(spark, graphPath + "/relatedPublications", RelatedPublication.class));
			final Dataset<OaBrokerMainEntity> r4 = join(
				r3, ClusterUtils.readPath(spark, graphPath + "/relatedSoftwares", RelatedSoftware.class));

			r4.write().mode(SaveMode.Overwrite).json(joinedEntitiesPath);

		});

	}

	private static <T> Dataset<OaBrokerMainEntity> join(final Dataset<OaBrokerMainEntity> sources,
		final Dataset<T> typedRels) {

		final TypedColumn<Tuple2<OaBrokerMainEntity, T>, OaBrokerMainEntity> aggr = new OaBrokerMainEntityAggregator<T>()
			.toColumn();

		return sources
			.joinWith(typedRels, sources.col("openaireId").equalTo(typedRels.col("source")), "left_outer")
			.groupByKey(
				(MapFunction<Tuple2<OaBrokerMainEntity, T>, String>) t -> t._1.getOpenaireId(), Encoders.STRING())
			.agg(aggr)
			.map(t -> t._2, Encoders.bean(OaBrokerMainEntity.class));

	}

}
