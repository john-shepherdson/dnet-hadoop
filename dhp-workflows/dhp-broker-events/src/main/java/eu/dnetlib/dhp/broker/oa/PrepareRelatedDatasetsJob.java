
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

import eu.dnetlib.broker.objects.OaBrokerRelatedDataset;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDataset;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareRelatedDatasetsJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelatedDatasetsJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareRelatedDatasetsJob.class
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

		final String relsPath = workingPath + "/relatedDatasets";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final Dataset<OaBrokerRelatedDataset> datasets = ClusterUtils
				.readPath(spark, graphPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class)
				.filter(d -> !ClusterUtils.isDedupRoot(d.getId()))
				.map(ConversionUtils::oafDatasetToBrokerDataset, Encoders.bean(OaBrokerRelatedDataset.class));

			final Dataset<Relation> rels = ClusterUtils
				.readPath(spark, graphPath + "/relation", Relation.class)
				.filter(r -> !ClusterUtils.isDedupRoot(r.getSource()))
				.filter(r -> !ClusterUtils.isDedupRoot(r.getTarget()));

			rels
				.joinWith(datasets, datasets.col("openaireId").equalTo(rels.col("target")), "inner")
				.map(
					t -> new RelatedDataset(
						t._1.getSource(),
						t._1.getRelType(),
						t._2),
					Encoders.bean(RelatedDataset.class))
				.write()
				.mode(SaveMode.Overwrite)
				.json(relsPath);

		});

	}

}
