
package eu.dnetlib.dhp.resulttocommunityfromproject;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
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
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import scala.Tuple2;

/**
 * @author miriam.baglioni
 * @Date 11/10/23
 */
public class SparkResultToCommunityFromProject implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkResultToCommunityFromProject.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToCommunityFromProject.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/resulttocommunityfromproject/input_communitytoresult_parameters.json"));

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

		Dataset<ResultProjectList> possibleUpdates = readPath(spark, possibleUpdatesPath, ResultProjectList.class);

		ModelSupport.entityTypes
			.keySet()
			.parallelStream()
			.filter(e -> ModelSupport.isResult(e))
			.forEach(e -> {
				// if () {
				removeOutputDir(spark, outputPath + e.name());
				Class<R> resultClazz = ModelSupport.entityTypes.get(e);
				Dataset<R> result = readPath(spark, inputPath + e.name(), resultClazz);

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

//				readPath(spark, outputPath + e.name(), resultClazz)
//					.write()
//					.mode(SaveMode.Overwrite)
//					.option("compression", "gzip")
//					.json(inputPath + e.name());
				// }
			});

	}

	private static <R extends Result> MapFunction<Tuple2<R, ResultProjectList>, R> resultCommunityFn() {
		return value -> {
			R ret = value._1();
			Optional<ResultProjectList> rcl = Optional.ofNullable(value._2());
			if (rcl.isPresent()) {
				// ArrayList<String> communitySet = rcl.get().getCommunityList();
				List<String> contextList = ret
					.getContext()
					.stream()
					.map(Context::getId)
					.collect(Collectors.toList());

				@SuppressWarnings("unchecked")
				R res = (R) ret.getClass().newInstance();

				res.setId(ret.getId());
				List<Context> propagatedContexts = new ArrayList<>();
				for (String cId : rcl.get().getCommunityList()) {
					if (!contextList.contains(cId)) {
						Context newContext = new Context();
						newContext.setId(cId);
						newContext
							.setDataInfo(
								Arrays
									.asList(
										getDataInfo(
											PROPAGATION_DATA_INFO_TYPE,
											PROPAGATION_RESULT_COMMUNITY_PROJECT_CLASS_ID,
											PROPAGATION_RESULT_COMMUNITY_PROJECT_CLASS_NAME,
											ModelConstants.DNET_PROVENANCE_ACTIONS)));
						propagatedContexts.add(newContext);
					} else {
						ret
							.getContext()
							.stream()
							.filter(c -> c.getId().equals(cId))
							.findFirst()
							.get()
							.getDataInfo()
							.add(
								getDataInfo(
									PROPAGATION_DATA_INFO_TYPE,
									PROPAGATION_RESULT_COMMUNITY_PROJECT_CLASS_ID,
									PROPAGATION_RESULT_COMMUNITY_PROJECT_CLASS_NAME,
									ModelConstants.DNET_PROVENANCE_ACTIONS));
					}
				}
				res.setContext(propagatedContexts);
				return MergeUtils.checkedMerge(ret, res, true);
			}
			return ret;
		};
	}
}
