
package eu.dnetlib.dhp.projecttoresult;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class SparkResultToProjectThroughSemRelJob {

	private static final Logger log = LoggerFactory.getLogger(SparkResultToProjectThroughSemRelJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToProjectThroughSemRelJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/projecttoresult/input_projecttoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final String potentialUpdatePath = parser.get("potentialUpdatePath");
		log.info("potentialUpdatePath {}: ", potentialUpdatePath);

		final String alreadyLinkedPath = parser.get("alreadyLinkedPath");
		log.info("alreadyLinkedPath {}: ", alreadyLinkedPath);

		final Boolean saveGraph = Boolean.valueOf(parser.get("saveGraph"));
		log.info("saveGraph: {}", saveGraph);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				if (isTest(parser)) {
					removeOutputDir(spark, outputPath);
				}
				execPropagation(
					spark, outputPath, alreadyLinkedPath, potentialUpdatePath);
			});
	}

	private static void execPropagation(
		SparkSession spark,
		String outputPath,
		String alreadyLinkedPath,
		String potentialUpdatePath) {

		Dataset<ResultProjectSet> toaddrelations = readPath(spark, potentialUpdatePath, ResultProjectSet.class);
		Dataset<ResultProjectSet> alreadyLinked = readPath(spark, alreadyLinkedPath, ResultProjectSet.class);

		// if (saveGraph) {
		toaddrelations
			.joinWith(
				alreadyLinked,
				toaddrelations.col("resultId").equalTo(alreadyLinked.col("resultId")),
				"left_outer")
			.flatMap(mapRelationRn(), Encoders.bean(Relation.class))
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(outputPath);
		// }
	}

	private static FlatMapFunction<Tuple2<ResultProjectSet, ResultProjectSet>, Relation> mapRelationRn() {
		return value -> {
			List<Relation> newRelations = new ArrayList<>();
			ResultProjectSet potentialUpdate = value._1();
			Optional<ResultProjectSet> alreadyLinked = Optional.ofNullable(value._2());
			alreadyLinked
				.ifPresent(
					resultProjectSet -> resultProjectSet
						.getProjectSet()
						.forEach(
							(p -> potentialUpdate.getProjectSet().remove(p))));
			String resId = potentialUpdate.getResultId();
			potentialUpdate
				.getProjectSet()
				.forEach(
					projectId -> {
						newRelations
							.add(
								getRelation(
									resId,
									projectId,
									ModelConstants.IS_PRODUCED_BY,
									ModelConstants.RESULT_PROJECT,
									ModelConstants.OUTCOME,
									PROPAGATION_DATA_INFO_TYPE,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
						newRelations
							.add(
								getRelation(
									projectId,
									resId,
									ModelConstants.PRODUCES,
									ModelConstants.RESULT_PROJECT,
									ModelConstants.OUTCOME,
									PROPAGATION_DATA_INFO_TYPE,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
					});
			return newRelations.iterator();
		};
	}

}
