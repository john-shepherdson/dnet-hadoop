
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

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublication;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareRelatedPublicationsJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelatedPublicationsJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		final String relsPath = workingPath + "/relatedPublications";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final Dataset<Publication> pubs = ClusterUtils
				.readPath(spark, graphPath + "/publication", Publication.class);

			final Dataset<Relation> rels = ClusterUtils.readPath(spark, graphPath + "/relation", Relation.class);

			rels
				.joinWith(pubs, pubs.col("id").equalTo(rels.col("target")), "inner")
				.map(
					t -> new RelatedPublication(
						t._1.getSource(),
						t._1.getRelType(),
						ConversionUtils.oafPublicationToBrokerPublication(t._2)),
					Encoders.bean(RelatedPublication.class))
				.write()
				.mode(SaveMode.Overwrite)
				.json(relsPath);

		});

	}

}
