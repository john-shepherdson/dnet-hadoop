
package eu.dnetlib.dhp.projecttoresult;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.countrypropagation.PrepareDatasourceCountryAssociation;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class SparkResultToProjectThroughSemRelJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareDatasourceCountryAssociation.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToProjectThroughSemRelJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/projecttoresult/input_projecttoresult_parameters.json"));

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
					spark, outputPath, alreadyLinkedPath, potentialUpdatePath, saveGraph);
			});
	}

	private static void execPropagation(
		SparkSession spark,
		String outputPath,
		String alreadyLinkedPath,
		String potentialUpdatePath,
		Boolean saveGraph) {

		Dataset<ResultProjectSet> toaddrelations = readPath(spark, potentialUpdatePath, ResultProjectSet.class);
		Dataset<ResultProjectSet> alreadyLinked = readPath(spark, alreadyLinkedPath, ResultProjectSet.class);

		if (saveGraph) {
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
		}
	}

	private static FlatMapFunction<Tuple2<ResultProjectSet, ResultProjectSet>, Relation> mapRelationRn() {
		return (FlatMapFunction<Tuple2<ResultProjectSet, ResultProjectSet>, Relation>) value -> {
			List<Relation> new_relations = new ArrayList<>();
			ResultProjectSet potential_update = value._1();
			Optional<ResultProjectSet> already_linked = Optional.ofNullable(value._2());
			if (already_linked.isPresent()) {
				already_linked
					.get()
					.getProjectSet()
					.stream()
					.forEach(
						(p -> {
							if (potential_update
								.getProjectSet()
								.contains(p)) {
								potential_update.getProjectSet().remove(p);
							}
						}));
			}
			String resId = potential_update.getResultId();
			potential_update
				.getProjectSet()
				.stream()
				.forEach(
					projectId -> {
						new_relations
							.add(
								getRelation(
									resId,
									projectId,
									RELATION_RESULT_PROJECT_REL_CLASS,
									RELATION_RESULTPROJECT_REL_TYPE,
									RELATION_RESULTPROJECT_SUBREL_TYPE,
									PROPAGATION_DATA_INFO_TYPE,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
						new_relations
							.add(
								getRelation(
									projectId,
									resId,
									RELATION_PROJECT_RESULT_REL_CLASS,
									RELATION_RESULTPROJECT_REL_TYPE,
									RELATION_RESULTPROJECT_SUBREL_TYPE,
									PROPAGATION_DATA_INFO_TYPE,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
									PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
					});
			return new_relations.iterator();
		};
	}

}
