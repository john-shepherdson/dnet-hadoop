
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.pidgraph.Entity;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkDumpPidResult implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkDumpPidResult.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpPidResult.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_dump_result.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("preparedInfoPath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpPidEntities(spark, inputPath, outputPath);

			});

	}

	private static void dumpPidEntities(SparkSession spark, String inputPath, String outputPath) {
		Dataset<ResultPidsList> resultPids = Utils.readPath(spark, inputPath, ResultPidsList.class);

		resultPids.flatMap((FlatMapFunction<ResultPidsList, Entity>) r -> {
			List<Entity> ret = new ArrayList<>();
			r.getResultAllowedPids().forEach(pid -> {
				if (StringUtils.isNoneEmpty(pid.getKey(), pid.getValue()))
					ret.add(Entity.newInstance(pid.getKey() + ":" + pid.getValue()));
			});
			return ret.iterator();
		}, Encoders.bean(Entity.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/result");
	}
}
