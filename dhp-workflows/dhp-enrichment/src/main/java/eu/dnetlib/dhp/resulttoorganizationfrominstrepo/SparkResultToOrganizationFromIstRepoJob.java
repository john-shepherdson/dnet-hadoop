
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
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class SparkResultToOrganizationFromIstRepoJob {

	private static final Logger log = LoggerFactory.getLogger(SparkResultToOrganizationFromIstRepoJob.class);

	private static final String RESULT_ORGANIZATIONSET_QUERY = "SELECT id resultId, collect_set(organizationId) organizationSet "
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

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToOrganizationFromIstRepoJob.class
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
				// removeOutputDir(spark, outputPath);
				if (saveGraph) {
					execPropagation(
						spark,
						datasourceorganization,
						alreadylinked,
						inputPath,
						outputPath,
						resultClazz);
				}
			});
	}

	private static void execPropagation(
		SparkSession spark,
		String datasourceorganization,
		String alreadyLinkedPath,
		String inputPath,
		String outputPath,
		Class<? extends Result> clazz) {

		Dataset<DatasourceOrganization> ds_org = readPath(spark, datasourceorganization, DatasourceOrganization.class);

		Dataset<ResultOrganizationSet> potentialUpdates = getPotentialRelations(spark, inputPath, clazz, ds_org);

		Dataset<ResultOrganizationSet> alreadyLinked = readPath(spark, alreadyLinkedPath, ResultOrganizationSet.class);

		potentialUpdates
			.joinWith(
				alreadyLinked,
				potentialUpdates.col("resultId").equalTo(alreadyLinked.col("resultId")),
				"left_outer")
			.flatMap(createRelationFn(), Encoders.bean(Relation.class))
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static FlatMapFunction<Tuple2<ResultOrganizationSet, ResultOrganizationSet>, Relation> createRelationFn() {
		return (FlatMapFunction<Tuple2<ResultOrganizationSet, ResultOrganizationSet>, Relation>) value -> {
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
									ModelConstants.IS_AUTHOR_INSTITUTION_OF,
									ModelConstants.RESULT_ORGANIZATION,
									ModelConstants.AFFILIATION,
									PROPAGATION_DATA_INFO_TYPE,
									PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID,
									PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
						new_relations
							.add(
								getRelation(
									resultId,
									orgId,
									ModelConstants.HAS_AUTHOR_INSTITUTION,
									ModelConstants.RESULT_ORGANIZATION,
									ModelConstants.AFFILIATION,
									PROPAGATION_DATA_INFO_TYPE,
									PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID,
									PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
					});
			return new_relations.iterator();
		};
	}

	private static <R extends Result> Dataset<ResultOrganizationSet> getPotentialRelations(
		SparkSession spark,
		String inputPath,
		Class<R> resultClazz,
		Dataset<DatasourceOrganization> ds_org) {

		Dataset<R> result = readPath(spark, inputPath, resultClazz);
		result.createOrReplaceTempView("result");
		createCfHbforResult(spark);

		ds_org.createOrReplaceTempView("rels");

		return spark
			.sql(RESULT_ORGANIZATIONSET_QUERY)
			.as(Encoders.bean(ResultOrganizationSet.class));
	}

}
