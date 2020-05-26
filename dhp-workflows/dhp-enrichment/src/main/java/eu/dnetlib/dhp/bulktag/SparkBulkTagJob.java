
package eu.dnetlib.dhp.bulktag;

import static eu.dnetlib.dhp.PropagationConstant.removeOutputDir;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bulktag.community.*;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkBulkTagJob {

	private static final Logger log = LoggerFactory.getLogger(SparkBulkTagJob.class);
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkBulkTagJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bulktag/input_bulkTag_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		Boolean isTest = Optional
			.ofNullable(parser.get("isTest"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);
		log.info("isTest: {} ", isTest);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		ProtoMap protoMappingParams = new Gson().fromJson(parser.get("pathMap"), ProtoMap.class);
		log.info("pathMap: {}", new Gson().toJson(protoMappingParams));

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final Boolean saveGraph = Optional
			.ofNullable(parser.get("saveGraph"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("saveGraph: {}", saveGraph);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		CommunityConfiguration cc;

		String taggingConf = parser.get("taggingConf");

		if (isTest) {
			cc = CommunityConfigurationFactory.newInstance(taggingConf);
		} else {
			cc = QueryInformationSystem.getCommunityConfiguration(parser.get("isLookUpUrl"));
		}

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				execBulkTag(spark, inputPath, outputPath, protoMappingParams, resultClazz, cc);
			});
	}

	private static <R extends Result> void execBulkTag(
		SparkSession spark,
		String inputPath,
		String outputPath,
		ProtoMap protoMappingParams,
		Class<R> resultClazz,
		CommunityConfiguration communityConfiguration) {

		ResultTagger resultTagger = new ResultTagger();
		readPath(spark, inputPath, resultClazz)
			.map(patchResult(), Encoders.bean(resultClazz))
			.map(
				(MapFunction<R, R>) value -> resultTagger
					.enrichContextCriteria(
						value, communityConfiguration, protoMappingParams),
				Encoders.bean(resultClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

	// TODO remove this hack as soon as the values fixed by this method will be provided as NON null
	private static <R extends Result> MapFunction<R, R> patchResult() {
		return (MapFunction<R, R>) r -> {
			if (r.getDataInfo().getDeletedbyinference() == null) {
				r.getDataInfo().setDeletedbyinference(false);
			}
			if (r.getContext() == null) {
				r.setContext(new ArrayList<>());
			}
			return r;
		};
	}

}
