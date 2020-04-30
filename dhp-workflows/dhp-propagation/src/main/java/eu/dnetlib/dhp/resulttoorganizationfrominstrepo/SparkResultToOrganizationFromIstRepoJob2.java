
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class SparkResultToOrganizationFromIstRepoJob2 {

	private static final Logger log = LoggerFactory.getLogger(SparkResultToOrganizationFromIstRepoJob2.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToOrganizationFromIstRepoJob2.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_propagationresulaffiliationfrominstrepo_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String datasourceorganization = parser.get("datasourceOrganizationPath");
		log.info("datasourceOrganizationPath: {}", datasourceorganization);

		final String alreadylinked = parser.get("alreadyLinkedPath");
		log.info("alreadyLinkedPath: {}", alreadylinked);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
		log.info("resultType: {}", resultType);

		final Boolean saveGraph = Optional
			.ofNullable(parser.get("saveGraph"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("saveGraph: {}", saveGraph);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				if (isTest(parser)) {
					removeOutputDir(spark, outputPath);
				}
				execPropagation(
					spark,
					datasourceorganization,
					alreadylinked,
					inputPath,
					outputPath,
					resultClazz,
					resultType,
					saveGraph);
			});
	}

	private static void execPropagation(
		SparkSession spark,
		String datasourceorganization,
		String alreadylinked,
		String inputPath,
		String outputPath,
		Class<? extends Result> resultClazz,
		String resultType,
		Boolean saveGraph) {
		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		org.apache.spark.sql.Dataset<DatasourceOrganization> datasourceorganizationassoc = readAssocDatasourceOrganization(
			spark, datasourceorganization);

		// broadcasting the result of the preparation step
		Broadcast<org.apache.spark.sql.Dataset<DatasourceOrganization>> broadcast_datasourceorganizationassoc = sc
			.broadcast(datasourceorganizationassoc);

		org.apache.spark.sql.Dataset<ResultOrganizationSet> potentialUpdates = getPotentialRelations(
			spark,
			inputPath,
			resultClazz,
			broadcast_datasourceorganizationassoc)
				.as(Encoders.bean(ResultOrganizationSet.class));

		if (saveGraph) {
			getNewRelations(
				spark
					.read()
					.textFile(alreadylinked)
					.map(
						value -> OBJECT_MAPPER
							.readValue(
								value, ResultOrganizationSet.class),
						Encoders.bean(ResultOrganizationSet.class)),
				potentialUpdates)
					.toJSON()
					.write()
					.mode(SaveMode.Append)
					.option("compression", "gzip")
					.text(outputPath);
		}
	}

	private static Dataset<Relation> getNewRelations(
		Dataset<ResultOrganizationSet> alreadyLinked,
		Dataset<ResultOrganizationSet> potentialUpdates) {

		return potentialUpdates
			.joinWith(
				alreadyLinked,
				potentialUpdates.col("resultId").equalTo(alreadyLinked.col("resultId")),
				"left_outer")
			.flatMap(
				(FlatMapFunction<Tuple2<ResultOrganizationSet, ResultOrganizationSet>, Relation>) value -> {
					List<Relation> new_relations = new ArrayList<>();
					ResultOrganizationSet potential_update = value._1();
					Optional<ResultOrganizationSet> already_linked = Optional.ofNullable(value._2());
					List<String> organization_list = potential_update.getOrganizationSet();
					if (already_linked.isPresent()) {
						already_linked
							.get()
							.getOrganizationSet()
							.stream()
							.forEach(
								rId -> {
									if (organization_list.contains(rId)) {
										organization_list.remove(rId);
									}
								});
					}
					String resultId = potential_update.getResultId();
					organization_list
						.stream()
						.forEach(
							orgId -> {
								new_relations
									.add(
										getRelation(
											orgId,
											resultId,
											RELATION_ORGANIZATION_RESULT_REL_CLASS,
											RELATION_RESULTORGANIZATION_REL_TYPE,
											RELATION_RESULTORGANIZATION_SUBREL_TYPE,
											PROPAGATION_DATA_INFO_TYPE,
											PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID,
											PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
								new_relations
									.add(
										getRelation(
											resultId,
											orgId,
											RELATION_RESULT_ORGANIZATION_REL_CLASS,
											RELATION_RESULTORGANIZATION_REL_TYPE,
											RELATION_RESULTORGANIZATION_SUBREL_TYPE,
											PROPAGATION_DATA_INFO_TYPE,
											PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID,
											PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
							});
					return new_relations.iterator();
				},
				Encoders.bean(Relation.class));
	}

	private static <R extends Result> org.apache.spark.sql.Dataset<ResultOrganizationSet> getPotentialRelations(
		SparkSession spark,
		String inputPath,
		Class<R> resultClazz,
		Broadcast<org.apache.spark.sql.Dataset<DatasourceOrganization>> broadcast_datasourceorganizationassoc) {
		org.apache.spark.sql.Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);
		result.createOrReplaceTempView("result");
		createCfHbforresult(spark);

		return organizationPropagationAssoc(spark, broadcast_datasourceorganizationassoc);
	}

	private static org.apache.spark.sql.Dataset<DatasourceOrganization> readAssocDatasourceOrganization(
		SparkSession spark, String datasourcecountryorganization) {
		return spark
			.read()
			.textFile(datasourcecountryorganization)
			.map(
				value -> OBJECT_MAPPER.readValue(value, DatasourceOrganization.class),
				Encoders.bean(DatasourceOrganization.class));
	}

	private static org.apache.spark.sql.Dataset<ResultOrganizationSet> organizationPropagationAssoc(
		SparkSession spark,
		Broadcast<org.apache.spark.sql.Dataset<DatasourceOrganization>> broadcast_datasourceorganizationassoc) {
		org.apache.spark.sql.Dataset<DatasourceOrganization> datasourceorganization = broadcast_datasourceorganizationassoc
			.value();
		datasourceorganization.createOrReplaceTempView("rels");
		String query = "SELECT id resultId, collect_set(organizationId) organizationSet "
			+ "FROM ( SELECT id, organizationId "
			+ "FROM rels "
			+ "JOIN cfhb "
			+ " ON cf = datasourceId     "
			+ "UNION ALL "
			+ "SELECT id , organizationId     "
			+ "FROM rels "
			+ "JOIN cfhb "
			+ " ON hb = datasourceId ) tmp "
			+ "GROUP BY id";
		return spark.sql(query).as(Encoders.bean(ResultOrganizationSet.class));
	}
}
