
package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class PrepareResultCommunitySetStep1 {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultCommunitySetStep1.class);

	/**
	 * associates to each result the set of community contexts they are associated to; associates to each target of a
	 * relation with allowed semantics the set of community context it could possibly inherit from the source of the
	 * relation
	 */
	// TODO
	private static final String RESULT_CONTEXT_QUERY_TEMPLATE = "select target resultId, community_context  "
		+ "from (select id, collect_set(co.id) community_context "
		+ "       from  result "
		+ "       lateral view explode (context) c as co "
		+ "       where datainfo.deletedbyinference = false %s group by id) p "
		+ " JOIN "
		+ " (select source, target from relation "
		+ "  where datainfo.deletedbyinference = false %s ) r ON p.id = r.source";

	/**
	 * a dataset for example could be linked to more than one publication. For each publication linked to that dataset
	 * the previous query will produce a row: targetId set of community context the target could possibly inherit with
	 * the following query there will be a single row for each result linked to more than one result of the result type
	 * currently being used
	 */
	// TODO
	private static final String RESULT_COMMUNITY_LIST_QUERY = "select resultId , collect_set(co) communityList "
		+ "from result_context "
		+ "lateral view explode (community_context) c as co "
		+ "where length(co) > 0 "
		+ "group by resultId";

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultCommunitySetStep1.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/resulttocommunityfromsemrel/input_preparecommunitytoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
		log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

		final String baseURL = parser.get("baseURL");
		log.info("baseURL: {}", baseURL);

		final List<String> communityIdList = getCommunityList(baseURL);
		log.info("communityIdList: {}", new Gson().toJson(communityIdList));

		final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
		log.info("resultType: {}", resultType);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				if (isTest(parser)) {
					removeOutputDir(spark, outputPath);
				}
				prepareInfo(
					spark,
					inputPath,
					outputPath,
					allowedsemrel,
					resultClazz,
					resultType,
					communityIdList);
			});
	}

	private static <R extends Result> void prepareInfo(
		SparkSession spark,
		String inputPath,
		String outputPath,
		List<String> allowedsemrel,
		Class<R> resultClazz,
		String resultType,
		List<String> communityIdList) {

		final String inputResultPath = inputPath + "/" + resultType;
		log.info("Reading Graph table from: {}", inputResultPath);

		final String inputRelationPath = inputPath + "/relation";
		log.info("Reading relation table from: {}", inputResultPath);

		Dataset<Relation> relation = readPath(spark, inputRelationPath, Relation.class);
		relation.createOrReplaceTempView("relation");

		Dataset<R> result = readPath(spark, inputResultPath, resultClazz);
		result.createOrReplaceTempView("result");

		final String outputResultPath = outputPath + "/" + resultType;
		log.info("writing output results to: {}", outputResultPath);

		String resultContextQuery = String
			.format(
				RESULT_CONTEXT_QUERY_TEMPLATE,
				getConstraintList(" lower(co.id) = '", communityIdList),
				getConstraintList(" lower(relClass) = '", allowedsemrel));

		Dataset<Row> result_context = spark.sql(resultContextQuery);
		result_context.createOrReplaceTempView("result_context");

		spark
			.sql(RESULT_COMMUNITY_LIST_QUERY)
			.as(Encoders.bean(ResultCommunityList.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputResultPath);
	}

	public static List<String> getCommunityList(final String baseURL) throws IOException {
		return Utils.getCommunityIdList(baseURL);
	}

}
