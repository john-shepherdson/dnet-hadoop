
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;

public class SparkResultToCommunityFromOrganizationJob2 {

	private static final Logger log = LoggerFactory.getLogger(SparkResultToCommunityFromOrganizationJob2.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToCommunityFromOrganizationJob2.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/resulttocommunityfromorganization/input_communitytoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String possibleupdatespath = parser.get("preparedInfoPath");
		log.info("preparedInfoPath: {}", possibleupdatespath);

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
				if (isTest(parser)) {
					removeOutputDir(spark, outputPath);
				}
				if (saveGraph)
					execPropagation(spark, inputPath, outputPath, resultClazz, possibleupdatespath);
			});
	}

	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String inputPath,
		String outputPath,
		Class<R> resultClazz,
		String possibleUpdatesPath) {
		org.apache.spark.sql.Dataset<ResultCommunityList> possibleUpdates = readResultCommunityList(
			spark, possibleUpdatesPath);
		org.apache.spark.sql.Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);

		result
			.joinWith(
				possibleUpdates,
				result.col("id").equalTo(possibleUpdates.col("resultId")),
				"left_outer")
			.map(
				value -> {
					R ret = value._1();
					Optional<ResultCommunityList> rcl = Optional.ofNullable(value._2());
					if (rcl.isPresent()) {
						ArrayList<String> communitySet = rcl.get().getCommunityList();
						List<String> contextList = ret
							.getContext()
							.stream()
							.map(con -> con.getId())
							.collect(Collectors.toList());
						Result res = new Result();
						res.setId(ret.getId());
						List<Context> propagatedContexts = new ArrayList<>();
						for (String cId : communitySet) {
							if (!contextList.contains(cId)) {
								Context newContext = new Context();
								newContext.setId(cId);
								newContext
									.setDataInfo(
										Arrays
											.asList(
												getDataInfo(
													PROPAGATION_DATA_INFO_TYPE,
													PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID,
													PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME)));
								propagatedContexts.add(newContext);
							}
						}
						res.setContext(propagatedContexts);
						ret.mergeFrom(res);
					}
					return ret;
				},
				Encoders.bean(resultClazz))
			.toJSON()
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.text(outputPath);
	}
}
