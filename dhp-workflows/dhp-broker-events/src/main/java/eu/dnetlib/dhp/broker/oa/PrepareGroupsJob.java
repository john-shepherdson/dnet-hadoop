
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
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultAggregator;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class PrepareGroupsJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareGroupsJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareGroupsJob.class
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

		final String groupsPath = workingPath + "/duplicates";
		log.info("groupsPath: {}", groupsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, groupsPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_groups");

			final Dataset<OaBrokerMainEntity> results = ClusterUtils
				.readPath(spark, workingPath + "/joinedEntities_step4", OaBrokerMainEntity.class);

			final Dataset<Relation> mergedRels = ClusterUtils
				.readPath(spark, graphPath + "/relation", Relation.class)
				.filter(r -> r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS));

			final TypedColumn<Tuple2<OaBrokerMainEntity, Relation>, ResultGroup> aggr = new ResultAggregator()
				.toColumn();

			final Dataset<ResultGroup> dataset = results
				.joinWith(mergedRels, results.col("openaireId").equalTo(mergedRels.col("source")), "inner")
				.groupByKey(
					(MapFunction<Tuple2<OaBrokerMainEntity, Relation>, String>) t -> t._2.getTarget(),
					Encoders.STRING())
				.agg(aggr)
				.map(t -> t._2, Encoders.bean(ResultGroup.class))
				.filter(rg -> rg.getData().size() > 1);

			ClusterUtils.save(dataset, groupsPath, ResultGroup.class, total);

		});
	}

}
