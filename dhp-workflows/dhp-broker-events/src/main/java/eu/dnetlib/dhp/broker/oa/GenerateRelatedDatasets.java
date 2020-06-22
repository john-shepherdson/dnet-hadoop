
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

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDataset;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class GenerateRelatedDatasets {

	private static final Logger log = LoggerFactory.getLogger(GenerateRelatedDatasets.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateRelatedDatasets.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/generate_relations.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String relsPath = parser.get("relsPath");
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> datasets = ClusterUtils
				.readPath(spark, graphPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class);

			final Dataset<Relation> rels = ClusterUtils.readPath(spark, graphPath + "/relation", Relation.class);

			rels
				.joinWith(datasets, datasets.col("id").equalTo(rels.col("target")), "inner")
				.map(
					t -> new RelatedDataset(
						t._1.getSource(),
						t._1.getRelType(),
						ConversionUtils.oafDatasetToBrokerDataset(t._2)),
					Encoders.bean(RelatedDataset.class))
				.write()
				.mode(SaveMode.Overwrite)
				.json(relsPath);

		});

	}

}
