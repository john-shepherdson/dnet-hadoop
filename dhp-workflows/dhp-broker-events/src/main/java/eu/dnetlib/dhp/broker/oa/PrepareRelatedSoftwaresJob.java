
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
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerRelatedSoftware;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftware;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;
import scala.Tuple2;

public class PrepareRelatedSoftwaresJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelatedSoftwaresJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareRelatedSoftwaresJob.class
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

		final String relsPath = workingDir + "/relatedSoftwares";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_rels");

			final Encoder<OaBrokerRelatedSoftware> obrsEncoder = Encoders.bean(OaBrokerRelatedSoftware.class);
			final Dataset<OaBrokerRelatedSoftware> softwares = ClusterUtils
				.readPath(spark, graphPath + "/software", Software.class)
				.filter((FilterFunction<Software>) sw -> !ClusterUtils.isDedupRoot(sw.getId()))
				.map(
					(MapFunction<Software, OaBrokerRelatedSoftware>) ConversionUtils::oafSoftwareToBrokerSoftware,
					obrsEncoder);

			final Dataset<Relation> rels;
			rels = ClusterUtils
				.loadRelations(graphPath, spark)
				.filter((FilterFunction<Relation>) r -> r.getRelType().equals(ModelConstants.RESULT_RESULT))
				.filter((FilterFunction<Relation>) r -> !r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS))
				.filter((FilterFunction<Relation>) r -> !ClusterUtils.isDedupRoot(r.getSource()))
				.filter((FilterFunction<Relation>) r -> !ClusterUtils.isDedupRoot(r.getTarget()));

			final Encoder<RelatedSoftware> rsEncoder = Encoders.bean(RelatedSoftware.class);
			final Dataset<RelatedSoftware> dataset = rels
				.joinWith(softwares, softwares.col("openaireId").equalTo(rels.col("target")), "inner")
				.map(
					(MapFunction<Tuple2<Relation, OaBrokerRelatedSoftware>, RelatedSoftware>) t -> new RelatedSoftware(
						t._1.getSource(), t._2),
					rsEncoder);

			ClusterUtils.save(dataset, relsPath, RelatedSoftware.class, total);

		});

	}

}
