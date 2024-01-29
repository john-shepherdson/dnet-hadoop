
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkSaveUnresolved implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkSaveUnresolved.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareFOSSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/createunresolvedentities/produce_unresolved_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String sourcePath = parser.get("sourcePath");
		log.info("sourcePath: {}", sourcePath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				saveUnresolved(
					spark,
					sourcePath,

					outputPath);
			});
	}

	private static void saveUnresolved(SparkSession spark, String sourcePath, String outputPath) {

		spark
			.read()
			.textFile(sourcePath + "/*")
			.map(
				(MapFunction<String, Result>) l -> OBJECT_MAPPER.readValue(l, Result.class),
				Encoders.bean(Result.class))
			.groupByKey((MapFunction<Result, String>) Result::getId, Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Result, Result>) (k, it) -> {
				Result ret = it.next();
				it.forEachRemaining(r -> {
//					if (r.getInstance() != null) {
//						ret.setInstance(r.getInstance());
//					}
					if (r.getSubject() != null) {
						if (ret.getSubject() != null)
							ret.getSubject().addAll(r.getSubject());
						else
							ret.setSubject(r.getSubject());
					}

					// ret.mergeFrom(r)
				});
				return ret;
			}, Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
