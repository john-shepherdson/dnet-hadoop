
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerProject;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedProject;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class PrepareRelatedProjectsJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelatedProjectsJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareRelatedProjectsJob.class
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

		final String relsPath = workingDir + "/relatedProjects";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_rels");

			final Dataset<OaBrokerProject> projects = ClusterUtils
				.readPath(spark, graphPath + "/project", Project.class)
				.filter((FilterFunction<Project>) p -> !ClusterUtils.isDedupRoot(p.getId()))
				.map(
					(MapFunction<Project, OaBrokerProject>) ConversionUtils::oafProjectToBrokerProject,
					Encoders.bean(OaBrokerProject.class));

			final Dataset<Relation> rels = ClusterUtils
				.loadRelations(graphPath, spark)
				.filter((FilterFunction<Relation>) r -> ModelConstants.RESULT_PROJECT.equals(r.getRelType()))
				.filter((FilterFunction<Relation>) r -> !BrokerConstants.IS_MERGED_IN_CLASS.equals(r.getRelClass()))
				.filter((FilterFunction<Relation>) r -> !ClusterUtils.isDedupRoot(r.getSource()))
				.filter((FilterFunction<Relation>) r -> !ClusterUtils.isDedupRoot(r.getTarget()));

			final Dataset<RelatedProject> dataset = rels
				.joinWith(projects, projects.col("openaireId").equalTo(rels.col("target")), "inner")
				.map(
					(MapFunction<Tuple2<Relation, OaBrokerProject>, RelatedProject>) t -> new RelatedProject(
						t._1.getSource(), t._2),
					Encoders.bean(RelatedProject.class));

			ClusterUtils.save(dataset, relsPath, RelatedProject.class, total);

		});

	}

}
