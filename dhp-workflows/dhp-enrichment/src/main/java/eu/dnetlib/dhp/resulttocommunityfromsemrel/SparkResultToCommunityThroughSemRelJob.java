
package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.*;
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
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class SparkResultToCommunityThroughSemRelJob {

	private static final Logger log = LoggerFactory.getLogger(SparkResultToCommunityThroughSemRelJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToCommunityThroughSemRelJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/resulttocommunityfromsemrel/input_communitytoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String preparedInfoPath = parser.get("preparedInfoPath");
		log.info("preparedInfoPath: {}", preparedInfoPath);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final Boolean saveGraph = Optional
			.ofNullable(parser.get("saveGraph"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("saveGraph: {}", saveGraph);

		@SuppressWarnings("unchecked")
		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				if (isTest(parser)) {
					removeOutputDir(spark, outputPath);
				}
				if (saveGraph) {
					execPropagation(
						spark, inputPath, outputPath, preparedInfoPath, resultClazz);
				}
			});
	}

	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String inputPath,
		String outputPath,
		String preparedInfoPath,
		Class<R> resultClazz) {

		Dataset<ResultCommunityList> possibleUpdates = readPath(spark, preparedInfoPath, ResultCommunityList.class);
		Dataset<R> result = readPath(spark, inputPath, resultClazz);

		result
			.joinWith(
				possibleUpdates,
				result.col("id").equalTo(possibleUpdates.col("resultId")),
				"left_outer")
			.map(contextUpdaterFn(), Encoders.bean(resultClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

		readPath(spark, outputPath, resultClazz)
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(inputPath);
	}

	private static <R extends Result> MapFunction<Tuple2<R, ResultCommunityList>, R> contextUpdaterFn() {
		return value -> {
			R ret = value._1();
			Optional<ResultCommunityList> rcl = Optional.ofNullable(value._2());
			if (rcl.isPresent()) {
				Set<String> contexts = new HashSet<>();
				ret.getContext().forEach(c -> contexts.add(c.getId()));
				List<Context> contextList = rcl
					.get()
					.getCommunityList()
					.stream()
					.map(
						c -> {
							if (!contexts.contains(c)) {
								Context newContext = new Context();
								newContext.setId(c);
								newContext
									.setDataInfo(
										Arrays
											.asList(
												getDataInfo(
													PROPAGATION_DATA_INFO_TYPE,
													PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
													PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
													ModelConstants.DNET_PROVENANCE_ACTIONS)));
								return newContext;
							}
							return null;
						})
					.filter(Objects::nonNull)
					.collect(Collectors.toList());

				@SuppressWarnings("unchecked")
				R r = (R) ret.getClass().newInstance();

				r.setId(ret.getId());
				r.setContext(contextList);
				ret.mergeFrom(r);
			}

			return ret;
		};
	}

}
