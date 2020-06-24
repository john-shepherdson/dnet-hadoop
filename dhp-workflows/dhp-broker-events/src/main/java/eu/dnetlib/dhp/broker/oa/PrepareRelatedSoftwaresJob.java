
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
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

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		final String relsPath = workingPath + "/relatedSoftwares";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final Dataset<OaBrokerRelatedSoftware> softwares = ClusterUtils
				.readPath(spark, graphPath + "/software", Software.class)
				.filter(sw -> !ClusterUtils.isDedupRoot(sw.getId()))
				.map(ConversionUtils::oafSoftwareToBrokerSoftware, Encoders.bean(OaBrokerRelatedSoftware.class));

			final Dataset<Relation> rels = ClusterUtils
				.readPath(spark, graphPath + "/relation", Relation.class)
				.filter(r -> r.getDataInfo().getDeletedbyinference())
				.filter(r -> r.getRelType().equals(ModelConstants.RESULT_RESULT))
				.filter(r -> !r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS))
				.filter(r -> !ClusterUtils.isDedupRoot(r.getSource()))
				.filter(r -> !ClusterUtils.isDedupRoot(r.getTarget()));

			rels
				.joinWith(softwares, softwares.col("openaireId").equalTo(rels.col("target")), "inner")
				.map(t -> new RelatedSoftware(t._1.getSource(), t._2), Encoders.bean(RelatedSoftware.class))
				.write()
				.mode(SaveMode.Overwrite)
				.json(relsPath);

		});

	}

}
