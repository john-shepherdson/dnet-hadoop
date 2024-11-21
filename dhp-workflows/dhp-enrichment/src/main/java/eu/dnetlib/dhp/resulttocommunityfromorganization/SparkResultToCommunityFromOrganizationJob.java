
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

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
import eu.dnetlib.dhp.schema.common.ModelSupport;
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
						"/eu/dnetlib/dhp/wf/subworkflows/resulttocommunityfromorganization/input_communitytoresult_parameters.json"));

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

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				execPropagation(spark, inputPath, outputPath, possibleupdatespath);

			});
	}

	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String inputPath,
		String outputPath,
		String possibleUpdatesPath) {

		Dataset<ResultCommunityList> possibleUpdates = readPath(spark, possibleUpdatesPath, ResultCommunityList.class);

		ModelSupport.entityTypes
			.keySet()
			.parallelStream()
			.filter(e -> ModelSupport.isResult(e))
			// .parallelStream()
			.forEach(e -> {
				// if () {
				Class<R> resultClazz = ModelSupport.entityTypes.get(e);
				removeOutputDir(spark, outputPath + e.name());
				Dataset<R> result = readPath(spark, inputPath + e.name(), resultClazz);

				log.info("executing left join");
				result
					.joinWith(
						possibleUpdates,
						result.col("id").equalTo(possibleUpdates.col("resultId")),
						"left_outer")
					.map(resultCommunityFn(), Encoders.bean(resultClazz))
					.write()
					.mode(SaveMode.Overwrite)
					.option("compression", "gzip")
					.json(outputPath + e.name());

//					log
//						.info(
//							"reading results from " + outputPath + e.name() + " and copying them to " + inputPath
//								+ e.name());
//					Dataset<R> tmp = readPath(spark, outputPath + e.name(), resultClazz);
//					if (tmp.count() > 0){
//
//						tmp
//								.write()
//								.mode(SaveMode.Overwrite)
//								.option("compression", "gzip")
//								.json(inputPath + e.name());
//					}

				// }
			});

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

				// @SuppressWarnings("unchecked")
				// R res = (R) ret.getClass().newInstance();

				// res.setId(ret.getId());
				// List<Context> propagatedContexts = new ArrayList<>();
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
						ret.getContext().add(newContext);
					}
				}
				// res.setContext(propagatedContexts);
				// return MergeUtils.mergeResult(ret, res);
			}
			return ret;
		};
	}

}
