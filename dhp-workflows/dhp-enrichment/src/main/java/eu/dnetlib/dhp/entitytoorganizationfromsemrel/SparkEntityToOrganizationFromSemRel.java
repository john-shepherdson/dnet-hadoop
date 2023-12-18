
package eu.dnetlib.dhp.entitytoorganizationfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttoorganizationfrominstrepo.SparkResultToOrganizationFromIstRepoJob;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SparkEntityToOrganizationFromSemRel implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkEntityToOrganizationFromSemRel.class);
	private static final int MAX_ITERATION = 5;
	public static final String NEW_RESULT_RELATION_PATH = "/newResultRelation";
	public static final String NEW_PROJECT_RELATION_PATH = "/newProjectRelation";

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToOrganizationFromIstRepoJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/input_propagation_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String relationPath = parser.get("relationPath");
		log.info("relationPath: {}", relationPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String leavesPath = parser.get("leavesPath");
		log.info("leavesPath: {}", leavesPath);

		final String childParentPath = parser.get("childParentPath");
		log.info("childParentPath: {}", childParentPath);

		final String resultOrganizationPath = parser.get("resultOrgPath");
		log.info("resultOrganizationPath: {}", resultOrganizationPath);

		final String projectOrganizationPath = parser.get("projectOrganizationPath");
		log.info("projectOrganizationPath: {}", projectOrganizationPath);

		final String workingPath = parser.get("workingDir");
		log.info("workingPath: {}", workingPath);

		final int iterations = Optional
			.ofNullable(parser.get("iterations"))
			.map(v -> {
				if (Integer.valueOf(v) < MAX_ITERATION) {
					return Integer.valueOf(v);
				} else
					return MAX_ITERATION;
			})
			.orElse(MAX_ITERATION);

		log.info("iterations: {}", iterations);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> execPropagation(
				spark,
				leavesPath,
				childParentPath,
				resultOrganizationPath,
				projectOrganizationPath,
				relationPath,
				workingPath,
				outputPath,
				iterations));
	}

	public static void execPropagation(SparkSession spark,
		String leavesPath,
		String childParentPath,
		String resultOrganizationPath,
		String projectOrganizationPath,
		String graphPath,
		String workingPath,
		String outputPath,
		int iterations) {
		if (iterations == 1) {
			doPropagateOnce(
				spark, leavesPath, childParentPath, resultOrganizationPath, projectOrganizationPath, graphPath,
				workingPath, outputPath);
		} else {

			final LongAccumulator iterationOne = spark.sparkContext().longAccumulator(ITERATION_ONE);
			final LongAccumulator iterationTwo = spark.sparkContext().longAccumulator(ITERATION_TWO);
			final LongAccumulator iterationThree = spark.sparkContext().longAccumulator(ITERATION_THREE);
			final LongAccumulator iterationFour = spark.sparkContext().longAccumulator(ITERATION_FOUR);
			final LongAccumulator iterationFive = spark.sparkContext().longAccumulator(ITERATION_FIVE);
			final LongAccumulator notReachedFirstParent = spark.sparkContext().longAccumulator(ITERATION_NO_PARENT);

			final PropagationCounter propagationCounter = new PropagationCounter(iterationOne,
				iterationTwo,
				iterationThree,
				iterationFour,
				iterationFive,
				notReachedFirstParent);

			doPropagate(
				spark, leavesPath, childParentPath, resultOrganizationPath, projectOrganizationPath, graphPath,
				workingPath, outputPath, propagationCounter);
		}

	}

	private static void doPropagateOnce(SparkSession spark, String leavesPath, String childParentPath,
		String resultOrganizationPath, String projectOrganizationPath, String graphPath, String workingPath,
		String outputPath) {

		StepActions
			.execStep(
				spark, graphPath + "/result", workingPath + NEW_RESULT_RELATION_PATH,
				leavesPath, childParentPath, resultOrganizationPath, ModelConstants.HAS_AUTHOR_INSTITUTION);

		addNewRelations(spark, workingPath + NEW_RESULT_RELATION_PATH, outputPath);

		StepActions
			.execStep(
				spark, graphPath + "/project", workingPath + NEW_PROJECT_RELATION_PATH,
				leavesPath, childParentPath, projectOrganizationPath, ModelConstants.HAS_PARTICIPANT);

		addNewRelations(spark, workingPath + NEW_PROJECT_RELATION_PATH, outputPath);
	}

	private static void doPropagate(SparkSession spark, String leavesPath, String childParentPath,
		String resultOrganizationPath, String projectOrganizationPath, String graphPath, String workingPath,
		String outputPath,
		PropagationCounter propagationCounter) {
		int iteration = 0;
		long leavesCount;

		do {
			iteration++;
			StepActions
				.execStep(
					spark, graphPath + "/result", workingPath + NEW_RESULT_RELATION_PATH,
					leavesPath, childParentPath, resultOrganizationPath, ModelConstants.HAS_AUTHOR_INSTITUTION);
			StepActions
				.execStep(
					spark, graphPath + "/project", workingPath + NEW_PROJECT_RELATION_PATH,
					leavesPath, childParentPath, projectOrganizationPath, ModelConstants.HAS_PARTICIPANT);

			StepActions
				.prepareForNextStep(
					spark, workingPath, resultOrganizationPath, projectOrganizationPath, leavesPath,
					childParentPath, workingPath + "/leaves", workingPath + "/resOrg", workingPath + "/projOrg");
			moveOutput(spark, workingPath, leavesPath, resultOrganizationPath, projectOrganizationPath);
			leavesCount = readPath(spark, leavesPath, Leaves.class).count();
		} while (leavesCount > 0 && iteration < MAX_ITERATION);

		if (leavesCount == 0) {
			switch (String.valueOf(iteration)) {
				case "1":
					propagationCounter.getIterationOne().add(1);
					break;
				case "2":
					propagationCounter.getIterationTwo().add(1);
					break;
				case "3":
					propagationCounter.getIterationThree().add(1);
					break;
				case "4":
					propagationCounter.getIterationFour().add(1);
					break;
				case "5":
					propagationCounter.getIterationFive().add(1);
					break;
				default:
					break;
			}
		} else {
			propagationCounter.getNotReachedFirstParent().add(1);
		}

		addNewRelations(spark, workingPath + NEW_RESULT_RELATION_PATH, outputPath);
		addNewRelations(spark, workingPath + NEW_PROJECT_RELATION_PATH, outputPath);
	}

	private static void moveOutput(SparkSession spark, String workingPath, String leavesPath,
		String resultOrganizationPath) {
		readPath(spark, workingPath + "/leaves", Leaves.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(leavesPath);

		readPath(spark, workingPath + "/resOrg", KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(resultOrganizationPath);

	}

	private static void moveOutput(SparkSession spark, String workingPath, String leavesPath,
		String resultOrganizationPath, String projectOrganizationPath) {
		readPath(spark, workingPath + "/leaves", Leaves.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(leavesPath);

		readPath(spark, workingPath + "/resOrg", KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(resultOrganizationPath);

		readPath(spark, workingPath + "/projOrg", KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(projectOrganizationPath);

	}

	private static void addNewRelations(SparkSession spark, String newRelationPath, String outputPath) {
		Dataset<Relation> relation = readPath(spark, newRelationPath, Relation.class);

		relation
			.groupByKey((MapFunction<Relation, String>) r -> r.getSource() + r.getTarget(), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Relation, Relation>) (k, it) -> it.next(), Encoders.bean(Relation.class))
			.flatMap(
				(FlatMapFunction<Relation, Relation>) r -> {
					if (r.getSource().startsWith("50|")) {
						return Arrays
							.asList(
								r, getAffiliationRelation(
									r.getTarget(), r.getSource(), ModelConstants.IS_AUTHOR_INSTITUTION_OF))
							.iterator();
					} else {
						return Arrays
							.asList(
								r, getParticipantRelation(
									r.getTarget(), r.getSource(), ModelConstants.IS_PARTICIPANT))
							.iterator();
					}
				}

				, Encoders.bean(Relation.class))
			.write()

			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
