
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;

public class SparkResultToCommunityFromOrganizationJob {

	private static final Logger log = LoggerFactory.getLogger(SparkResultToCommunityFromOrganizationJob.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToCommunityFromOrganizationJob.class
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



		@SuppressWarnings("unchecked")
		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {

					execPropagation(spark, inputPath, outputPath, resultClazz, possibleupdatespath);

			});
	}

	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String inputPath,
		String outputPath,
		Class<R> resultClazz,
		String possibleUpdatesPath) {

		Dataset<ResultCommunityList> possibleUpdates = readPath(spark, possibleUpdatesPath, ResultCommunityList.class);
		Dataset<R> result = readPath(spark, inputPath, resultClazz);

		result
			.joinWith(
				possibleUpdates,
				result.col("id").equalTo(possibleUpdates.col("resultId")),
				"left_outer")
			.map(resultCommunityFn(), Encoders.bean(resultClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

		readPath(spark, outputPath, resultClazz)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(inputPath);
	}

	private static <R extends Result> MapFunction<Tuple2<R, ResultCommunityList>, R> resultCommunityFn() {
		return value -> {
			R ret = value._1();
			Optional<ResultCommunityList> rcl = Optional.ofNullable(value._2());
			if (rcl.isPresent()) {
				ArrayList<String> communitySet = rcl.get().getCommunityList();
				List<String> contextList = ret
					.getContext()
					.stream()
					.map(Context::getId)
					.collect(Collectors.toList());

				@SuppressWarnings("unchecked")
				R res = (R) ret.getClass().newInstance();

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
											PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME,
											ModelConstants.DNET_PROVENANCE_ACTIONS)));
						propagatedContexts.add(newContext);
					}
				}
				res.setContext(propagatedContexts);
				ret.mergeFrom(res);
			}
			return ret;
		};
	}

}
