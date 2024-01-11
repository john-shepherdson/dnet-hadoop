
package eu.dnetlib.dhp.projecttoresult;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareProjectResultsAssociation {
	private static final Logger log = LoggerFactory.getLogger(PrepareProjectResultsAssociation.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareProjectResultsAssociation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/projecttoresult/input_prepareprojecttoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String potentialUpdatePath = parser.get("potentialUpdatePath");
		log.info("potentialUpdatePath {}: ", potentialUpdatePath);

		String alreadyLinkedPath = parser.get("alreadyLinkedPath");
		log.info("alreadyLinkedPath: {} ", alreadyLinkedPath);

		final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
		log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, potentialUpdatePath);
				removeOutputDir(spark, alreadyLinkedPath);
				prepareResultProjProjectResults(
					spark,
					inputPath,
					potentialUpdatePath,
					alreadyLinkedPath,
					allowedsemrel);
			});
	}

	private static void prepareResultProjProjectResults(
		SparkSession spark,
		String inputPath,
		String potentialUpdatePath,
		String alreadyLinkedPath,
		List<String> allowedsemrel) {

		Dataset<Relation> relation = readPath(spark, inputPath, Relation.class);
		relation.createOrReplaceTempView("relation");

		String resproj_relation_query = "SELECT source, target "
			+ "       FROM relation "
			+ "       WHERE datainfo.deletedbyinference = false "
			+ "       AND lower(relClass) = '"
			+ ModelConstants.IS_PRODUCED_BY.toLowerCase()
			+ "'";

		Dataset<Row> resproj_relation = spark.sql(resproj_relation_query);
		resproj_relation.createOrReplaceTempView("resproj_relation");

		String potential_update_query = "SELECT resultId, collect_set(projectId) projectSet "
			+ "FROM ( "
			+ "SELECT r1.target resultId, r2.target projectId "
			+ "      FROM (SELECT source, target "
			+ "            FROM relation "
			+ "            WHERE datainfo.deletedbyinference = false  "
			+ getConstraintList(" lower(relClass) = '", allowedsemrel)
			+ "            ) r1"
			+ "      JOIN resproj_relation r2 "
			+ "      ON r1.source = r2.source "
			+ "      ) tmp "
			+ "GROUP BY resultId ";

		spark
			.sql(potential_update_query)
			.as(Encoders.bean(ResultProjectSet.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(potentialUpdatePath);

		String result_projectset_query = "SELECT source resultId, collect_set(target) projectSet "
			+ "FROM resproj_relation "
			+ "GROUP BY source";

		spark
			.sql(result_projectset_query)
			.as(Encoders.bean(ResultProjectSet.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(alreadyLinkedPath);
	}

}
