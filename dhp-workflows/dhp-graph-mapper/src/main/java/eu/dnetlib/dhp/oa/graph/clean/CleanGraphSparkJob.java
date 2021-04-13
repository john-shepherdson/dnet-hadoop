
package eu.dnetlib.dhp.oa.graph.clean;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.oaf.CleaningFunctions.*;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class CleanGraphSparkJob {

	private static final Logger log = LoggerFactory.getLogger(CleanGraphSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CleanGraphSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);
		final VocabularyGroup vocs = VocabularyGroup.loadVocsFromIS(isLookupService);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				cleanGraphTable(spark, vocs, inputPath, entityClazz, outputPath);
			});
	}

	private static <T extends Oaf> void cleanGraphTable(
		SparkSession spark,
		VocabularyGroup vocs,
		String inputPath,
		Class<T> clazz,
		String outputPath) {

		final CleaningRuleMap mapping = CleaningRuleMap.create(vocs);

		readTableFromPath(spark, inputPath, clazz)
			.map((MapFunction<T, T>) value -> fixVocabularyNames(value), Encoders.bean(clazz))
			.map((MapFunction<T, T>) value -> OafCleaner.apply(value, mapping), Encoders.bean(clazz))
			.map((MapFunction<T, T>) value -> cleanup(value), Encoders.bean(clazz))
			.filter((FilterFunction<T>) value -> filter(value))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static <T extends Oaf> Dataset<T> readTableFromPath(
		SparkSession spark, String inputEntityPath, Class<T> clazz) {

		log.info("Reading Graph table from: {}", inputEntityPath);
		return spark
			.read()
			.textFile(inputEntityPath)
			.map(
				(MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, clazz),
				Encoders.bean(clazz));
	}

}
