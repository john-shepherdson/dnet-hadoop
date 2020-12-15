
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

import eu.dnetlib.broker.objects.OaBrokerRelatedDataset;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDataset;
import eu.dnetlib.dhp.schema.common.ModelConstants;
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

		final String workingDir = parser.get("workingDir");
		log.info("workingDir: {}", workingDir);

		final String relsPath = workingDir + "/relatedDatasets";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_rels");

			final Dataset<OaBrokerRelatedDataset> datasets = ClusterUtils
				.readPath(spark, graphPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class)
				.filter(d -> !ClusterUtils.isDedupRoot(d.getId()))
				.map(ConversionUtils::oafDatasetToBrokerDataset, Encoders.bean(OaBrokerRelatedDataset.class));

			final Dataset<Relation> rels = ClusterUtils
				.loadRelations(graphPath, spark)
				.filter(r -> r.getDataInfo().getDeletedbyinference())
				.filter(r -> r.getRelType().equals(ModelConstants.RESULT_RESULT))
				.filter(r -> ClusterUtils.isValidResultResultClass(r.getRelClass()))
				.filter(r -> !ClusterUtils.isDedupRoot(r.getSource()))
				.filter(r -> !ClusterUtils.isDedupRoot(r.getTarget()));

			final Dataset<RelatedDataset> dataset = rels
				.joinWith(datasets, datasets.col("openaireId").equalTo(rels.col("target")), "inner")
				.map(t -> {
					final RelatedDataset rel = new RelatedDataset(t._1.getSource(),
						t._2);
					rel.getRelDataset().setRelType(t._1.getRelClass());
					return rel;
				}, Encoders.bean(RelatedDataset.class));

			ClusterUtils.save(dataset, relsPath, RelatedDataset.class, total);

		});

	}

}
