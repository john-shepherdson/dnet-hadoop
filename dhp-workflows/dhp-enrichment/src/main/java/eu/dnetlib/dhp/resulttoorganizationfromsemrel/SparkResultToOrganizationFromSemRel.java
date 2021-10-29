
package eu.dnetlib.dhp.resulttoorganizationfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.Arrays;

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
import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttoorganizationfrominstrepo.SparkResultToOrganizationFromIstRepoJob;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SparkResultToOrganizationFromSemRel implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkResultToOrganizationFromSemRel.class);
	private static final int MAX_ITERATION = 5;

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToOrganizationFromIstRepoJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/resulttoorganizationfromsemrel/input_propagation_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String leavesPath = parser.get("leavesPath");
		log.info("leavesPath: {}", leavesPath);

		final String childParentPath = parser.get("childParentPath");
		log.info("childParentPath: {}", childParentPath);

		final String resultOrganizationPath = parser.get("resultOrgPath");
		log.info("resultOrganizationPath: {}", resultOrganizationPath);

		final String workingPath = parser.get("workingDir");
		log.info("workingPath: {}", workingPath);

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
				graphPath,
				workingPath,
				outputPath));
	}

	public static void execPropagation(SparkSession spark,
		String leavesPath,
		String childParentPath,
		String resultOrganizationPath,
		String graphPath,
		String workingPath,
		String outputPath) {

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
			spark, leavesPath, childParentPath, resultOrganizationPath, graphPath,
			workingPath, outputPath, propagationCounter);

	}

	private static void doPropagate(SparkSession spark, String leavesPath, String childParentPath,
		String resultOrganizationPath, String graphPath, String workingPath, String outputPath,
		PropagationCounter propagationCounter) {
		int iteration = 0;
		long leavesCount = 0;

		do {
			iteration++;
			StepActions
				.execStep(
					spark, graphPath, workingPath + "/newRelation",
					leavesPath, childParentPath, resultOrganizationPath);
			StepActions
				.prepareForNextStep(
					spark, workingPath + "/newRelation", resultOrganizationPath, leavesPath,
					childParentPath, workingPath + "/leaves", workingPath + "/resOrg");
			moveOutput(spark, workingPath, leavesPath, resultOrganizationPath);
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

		addNewRelations(spark, workingPath + "/newRelation", outputPath);
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

	private static void addNewRelations(SparkSession spark, String newRelationPath, String outputPath) {
		Dataset<Relation> relation = readPath(spark, newRelationPath, Relation.class);

		relation
			.groupByKey((MapFunction<Relation, String>) r -> r.getSource() + r.getTarget(), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Relation, Relation>) (k, it) -> it.next(), Encoders.bean(Relation.class))
			.flatMap(
				(FlatMapFunction<Relation, Relation>) r -> Arrays
					.asList(
						r, getRelation(
							r.getTarget(), r.getSource(), ModelConstants.IS_AUTHOR_INSTITUTION_OF,
							ModelConstants.RESULT_ORGANIZATION,
							ModelConstants.AFFILIATION,
							PROPAGATION_DATA_INFO_TYPE,
							PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID,
							PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME))
					.iterator()

				, Encoders.bean(Relation.class))
			.write()

			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(outputPath);
	}

	// per ogni figlio nel file delle organizzazioni
	// devo fare una roba iterativa che legge info da un file e le cambia via via
	// passo 1: creo l'informazione iniale: organizzazioni che non hanno figli con almeno un padre
	// ogni organizzazione punta alla lista di padri
	// eseguo la propagazione dall'organizzazione figlio all'organizzazione padre
	// ricerco nel dataset delle relazioni se l'organizzazione a cui ho propagato ha, a sua volta, dei padri
	// e propago anche a quelli e cosi' via fino a che arrivo ad organizzazione senza padre

	// organizationFile:
	// f => {p1, p2, ..., pn}
	// resultFile
	// o => {r1, r2, ... rm}

	// supponiamo che f => {r1, r2} e che nessuno dei padri abbia gia' l'associazione con questi result
	// quindi
	// p1 => {r1, r2}
	// p2 => {r1, r2}
	// pn => {r1, r2}

	// mi serve un file con tutta la gerarchia per organizzazioni
	// un file con le organizzazioni foglia da joinare con l'altro file
	// un file con le associazioni organizzazione -> result forse meglio result -> organization

	// eseguo gli step fino a che ho foglie nel set
	// quando non ne ho piu' creo relazioni doppio verso per le nuove propagate che ho introdotto

}
