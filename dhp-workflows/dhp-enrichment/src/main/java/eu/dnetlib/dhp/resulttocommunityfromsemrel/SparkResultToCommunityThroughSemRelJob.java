
package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.bulktag.community.ResultTagger;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import net.sf.saxon.functions.Remove;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
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
						"/eu/dnetlib/dhp/wf/subworkflows/resulttocommunityfromsemrel/input_communitytoresult_parameters.json"));

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

		final String removeContextPath = parser.get("removeContextPath");
		log.info("removeContextPath: {}", removeContextPath);


		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				execPropagation(
						spark, inputPath, outputPath, preparedInfoPath, removeContextPath);

			}
		);
	}


	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String inputPath,
		String outputPath,
		String preparedInfoPath,
		String removeContextPath) {

		Dataset<ResultCommunityList> possibleUpdates = readPath(spark, preparedInfoPath, ResultCommunityList.class);
		ModelSupport.entityTypes
				.keySet()
				.parallelStream()
				.filter(ModelSupport::isResult)
				.forEach(e -> {
					removeOutputDir(spark, outputPath + e.name());
					ResultTagger resultTagger = new ResultTagger();
					Class<R> resultClazz = ModelSupport.entityTypes.get(e);
					Dataset<R> result = readPath(spark, inputPath + e.name(), resultClazz);
					result
							.joinWith(
									possibleUpdates,
									result.col("id").equalTo(possibleUpdates.col("resultId")),
									"left_outer")
							.map(contextUpdaterFn(), Encoders.bean(resultClazz))
							.write()
							.mode(SaveMode.Overwrite)
							.option("compression", "gzip")
							.json(outputPath + e.name() + "_removed");

					Dataset<RemovePojo> toRemove = spark.read().schema(Encoders.tuple(Encoders.STRING(), Encoders.STRING()).schema()).json(removeContextPath + e.name())
							.groupByKey((MapFunction<Row, String>) r -> (String) r.getAs("_1"), Encoders.STRING())
							.mapGroups((MapGroupsFunction<String, Row, RemovePojo>) (k, it) -> {
								List<String> ret = new ArrayList<>();
								it.forEachRemaining(community -> ret.add(community.getAs("_2")));
								return new RemovePojo(k, ret);
							}, Encoders.bean(RemovePojo.class));
					result.joinWith(toRemove, result.col("id").equalTo(toRemove.col("resultId")), "left")
							.map((MapFunction<Tuple2<R, RemovePojo>, R>) t2 -> {
								R r = t2._1();
								if(t2._2() != null){
									r.setContext(removeContextIds(t2._2().getContextList(), r.getContext()));
								}
								return r;
							}, Encoders.bean(resultClazz))
							.write()
							.mode(SaveMode.Overwrite)
							.option("compression","gzip")
							.json(outputPath + e.name());
				});
	}

	private static List<Context> removeContextIds(List<String> context, List<Context> contextList) {
		return contextList.stream().filter(c -> context.stream().noneMatch(rc -> rc.equalsIgnoreCase(c.getId())))
				.collect(Collectors.toList());
	}

	private static <R extends Result> MapFunction<Tuple2<R, ResultCommunityList>, R> contextUpdaterFn() {
		return value -> {
			R ret = value._1();
			Optional<ResultCommunityList> rcl = Optional.ofNullable(value._2());
			if (rcl.isPresent()) {
				Set<String> contexts = new HashSet<>();
				ret.getContext().forEach(c -> contexts.add(c.getId()));
				rcl
					.get()
					.getCommunityList()
					.stream()
					.forEach(
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
								ret.getContext().add(newContext);
							}

						});

			}

			return ret;
		};
	}

}
class RemovePojo {
	String resultId;
	List<String> contextList;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public List<String> getContextList() {
		return contextList;
	}

	public void setContextList(List<String> contextList) {
		this.contextList = contextList;
	}

	public RemovePojo(String resultId, List<String> contextList) {
		this.resultId = resultId;
		this.contextList = contextList;
	}
}