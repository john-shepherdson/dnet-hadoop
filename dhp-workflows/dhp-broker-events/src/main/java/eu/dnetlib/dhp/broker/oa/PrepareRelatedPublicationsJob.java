
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerRelatedPublication;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublication;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareRelatedPublicationsJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelatedPublicationsJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareRelatedPublicationsJob.class
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

		final String relsPath = workingDir + "/relatedPublications";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_rels");

			final Dataset<OaBrokerRelatedPublication> pubs = ClusterUtils
				.readPath(spark, graphPath + "/publication", Publication.class)
				.filter(p -> !ClusterUtils.isDedupRoot(p.getId()))
				.map(
					ConversionUtils::oafPublicationToBrokerPublication,
					Encoders.bean(OaBrokerRelatedPublication.class));

			final Dataset<Relation> rels = ClusterUtils
				.readPath(spark, graphPath + "/relation", Relation.class)
				.filter(r -> r.getDataInfo().getDeletedbyinference())
				.filter(r -> r.getRelType().equals(ModelConstants.RESULT_RESULT))
				.filter(r -> ClusterUtils.isValidResultResultClass(r.getRelClass()))
				.filter(r -> !ClusterUtils.isDedupRoot(r.getSource()))
				.filter(r -> !ClusterUtils.isDedupRoot(r.getTarget()));

			final Dataset<RelatedPublication> dataset = rels
				.joinWith(pubs, pubs.col("openaireId").equalTo(rels.col("target")), "inner")
				.map(t -> {
					final RelatedPublication rel = new RelatedPublication(t._1.getSource(), t._2);
					rel.getRelPublication().setRelType(t._1.getRelClass());
					return rel;
				}, Encoders.bean(RelatedPublication.class));

			ClusterUtils.save(dataset, relsPath, RelatedPublication.class, total);

		});

	}

}
