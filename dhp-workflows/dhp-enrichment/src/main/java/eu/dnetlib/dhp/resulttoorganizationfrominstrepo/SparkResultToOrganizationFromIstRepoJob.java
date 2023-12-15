
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

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

import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;

public class SparkResultToOrganizationFromIstRepoJob {

	private static final Logger log = LoggerFactory.getLogger(SparkResultToOrganizationFromIstRepoJob.class);

	private static final String RESULT_ORGANIZATIONSET_QUERY = "SELECT id key, collect_set(organizationId) valueSet "
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

		Dataset<DatasourceOrganization> dsOrg = readPath(spark, datasourceorganization, DatasourceOrganization.class);

		Dataset<KeyValueSet> potentialUpdates = getPotentialRelations(spark, inputPath, clazz, dsOrg);

		Dataset<KeyValueSet> alreadyLinked = readPath(spark, alreadyLinkedPath, KeyValueSet.class);

		potentialUpdates
			.joinWith(
				alreadyLinked,
				potentialUpdates.col("key").equalTo(alreadyLinked.col("key")),
				"left_outer")
			.flatMap(createRelationFn(), Encoders.bean(Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static FlatMapFunction<Tuple2<KeyValueSet, KeyValueSet>, Relation> createRelationFn() {
		return value -> {
			List<Relation> newRelations = new ArrayList<>();
			KeyValueSet potentialUpdate = value._1();
			Optional<KeyValueSet> alreadyLinked = Optional.ofNullable(value._2());
			List<String> organizations = potentialUpdate.getValueSet();
			alreadyLinked
				.ifPresent(
					resOrg -> resOrg
						.getValueSet()
						.forEach(organizations::remove));
			String resultId = potentialUpdate.getKey();
			organizations
				.forEach(
					orgId -> newRelations
						.addAll(
							getOrganizationRelationPair(
								orgId,
								resultId,
								PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID,
								PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME))

				);
			return newRelations.iterator();
		};
	}

	private static <R extends Result> Dataset<KeyValueSet> getPotentialRelations(
		SparkSession spark,
		String inputPath,
		Class<R> resultClazz,
		Dataset<DatasourceOrganization> dsOrg) {

		Dataset<R> result = readPath(spark, inputPath, resultClazz);
		result.createOrReplaceTempView("result");
		createCfHbforResult(spark);

		dsOrg.createOrReplaceTempView("rels");

		return spark
			.sql(RESULT_ORGANIZATIONSET_QUERY)
			.as(Encoders.bean(KeyValueSet.class));
	}

}
