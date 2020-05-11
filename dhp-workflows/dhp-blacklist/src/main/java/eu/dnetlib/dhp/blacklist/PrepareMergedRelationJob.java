
package eu.dnetlib.dhp.blacklist;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

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

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareMergedRelationJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareMergedRelationJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareMergedRelationJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/blacklist/input_preparerelation_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {} ", outputPath);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				selectMergesRelations(
					spark,
					inputPath,
					outputPath);
			});
	}

	private static void selectMergesRelations(SparkSession spark, String inputPath, String outputPath) {

		Dataset<Relation> relation = readRelations(spark, inputPath);

		relation
			.filter("relclass = 'merges' and datainfo.deletedbyinference=false")
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
//		relation.createOrReplaceTempView("relation");
//
//		spark
//			.sql(
//				"Select * from relation " +
//					"where relclass = 'merges' " +
//					"and datainfo.deletedbyinference = false")
//			.as(Encoders.bean(Relation.class))
//			.toJSON()
//			.write()
//			.mode(SaveMode.Overwrite)
//			.option("compression", "gzip")
//			.text(outputPath);
	}

	public static org.apache.spark.sql.Dataset<Relation> readRelations(
		SparkSession spark, String inputPath) {
		return spark
			.read()
			.textFile(inputPath)
			.map(
				(MapFunction<String, Relation>) value -> OBJECT_MAPPER.readValue(value, Relation.class),
				Encoders.bean(Relation.class));
	}
}
