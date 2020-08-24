
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.rmi.CORBA.Util;

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

public class SparkCollectPreparedInfo implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkCollectPreparedInfo.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkCollectPreparedInfo.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_collectandsave.json"));

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
				collectAndSave(spark, inputPath, outputPath);

			});

	}

	private static void collectAndSave(SparkSession spark, String inputPath, String outputPath) {

		Utils
			.readPath(spark, inputPath + "/publication", ResultPidsList.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", ResultPidsList.class))
			.union(Utils.readPath(spark, inputPath + "/software", ResultPidsList.class))
			.union(Utils.readPath(spark, inputPath + "/otherresearchproduct", ResultPidsList.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
