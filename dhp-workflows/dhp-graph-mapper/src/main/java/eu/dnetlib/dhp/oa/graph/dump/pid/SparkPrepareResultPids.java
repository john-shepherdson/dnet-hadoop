
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkPrepareResultPids implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkPrepareResultPids.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkPrepareResultPids.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final List<String> allowedResultPid = new Gson().fromJson(parser.get("allowedResultPids"), List.class);
		final List<String> allowedAuthorPid = new Gson().fromJson(parser.get("allowedAuthorPids"), List.class);

		final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
		log.info("resultType: {}", resultType);

		Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				preparePidEntities(
					spark, inputPath, outputPath + "/" + resultType, inputClazz, allowedResultPid, allowedAuthorPid);

			});

	}

	private static <R extends Result> void preparePidEntities(SparkSession spark, String inputPath, String outputPath,
		Class<R> inputClazz, List<String> allowedResultPid,
		List<String> allowedAuthorPid) {

		Dataset<R> result = Utils.readPath(spark, inputPath, inputClazz);

		result.map((MapFunction<R, ResultPidsList>) res -> {
			ResultPidsList ret = new ResultPidsList();
			ret.setResultId(res.getId());
			List<KeyValue> pidList = new ArrayList<>();
			Optional
				.ofNullable(res.getPid())
				.ifPresent(pids -> pids.forEach(pid -> {
					if (allowedResultPid.contains(pid.getQualifier().getClassid().toLowerCase())) {
						pidList.add(KeyValue.newInstance(pid.getQualifier().getClassid(), pid.getValue()));
					}
				}));
			ret.setResultAllowedPids(pidList);
			List<List<KeyValue>> authorPidList = new ArrayList<>();
			Optional
				.ofNullable(res.getAuthor())
				.ifPresent(authors -> authors.forEach(a -> {
					Optional
						.ofNullable(a.getPid())
						.ifPresent(pids -> pids.forEach(p -> {
							List<KeyValue> authorPids = new ArrayList<>();
							if (allowedAuthorPid.contains(p.getQualifier().getClassid().toLowerCase())) {
								authorPids.add(KeyValue.newInstance(p.getQualifier().getClassid(), p.getValue()));
							}
							if (authorPids.size() > 0) {
								authorPidList.add(authorPids);
							}
						}));
				}));
			ret.setAuthorAllowedPids(authorPidList);

			if (authorPidList.size() == 0 && pidList.size() == 0) {
				return null;
			}
			return ret;
		}, Encoders.bean(ResultPidsList.class))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
