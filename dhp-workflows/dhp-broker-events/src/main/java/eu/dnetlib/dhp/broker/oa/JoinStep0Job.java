
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.AddDatasourceTypeAggregator;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.SimpleDatasourceInfo;
import scala.Tuple2;

public class JoinStep0Job {

	private static final Logger log = LoggerFactory.getLogger(JoinStep0Job.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					JoinStep0Job.class
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

		final String outputPath = workingPath + "/joinedEntities_step0";
		log.info("outputPath: {}", outputPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, outputPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_entities");

			final Dataset<OaBrokerMainEntity> sources = ClusterUtils
				.readPath(spark, workingPath + "/simpleEntities", OaBrokerMainEntity.class);

			final Dataset<SimpleDatasourceInfo> datasources = ClusterUtils
				.readPath(spark, workingPath + "/datasources", SimpleDatasourceInfo.class);

			final TypedColumn<Tuple2<OaBrokerMainEntity, SimpleDatasourceInfo>, OaBrokerMainEntity> aggr = new AddDatasourceTypeAggregator()
				.toColumn();

			final Dataset<OaBrokerMainEntity> dataset = sources
				.joinWith(datasources, sources.col("collectedFromId").equalTo(datasources.col("id")), "inner")
				.groupByKey(t -> t._1.getOpenaireId(), Encoders.STRING())
				.agg(aggr)
				.map(t -> t._2, Encoders.bean(OaBrokerMainEntity.class));

			ClusterUtils.save(dataset, outputPath, OaBrokerMainEntity.class, total);

		});

	}

}
