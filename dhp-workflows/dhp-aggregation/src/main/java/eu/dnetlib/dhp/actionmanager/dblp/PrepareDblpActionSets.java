
package eu.dnetlib.dhp.actionmanager.dblp;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.actionmanager.bipaffiliations.PrepareAffiliationRelations;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

/**
 * Creates action sets for DBLP data
 */
public class PrepareDblpActionSets implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareAffiliationRelations.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________::";
	public static final String BIP_AFFILIATIONS_CLASSID = "result:organization:bipinference";
	public static final String BIP_AFFILIATIONS_CLASSNAME = "Affiliation relation inferred by BIP!";
	public static final String BIP_INFERENCE_PROVENANCE = "bip:affiliation:crossref";

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareAffiliationRelations.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/dblp/input_actionset_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Constants.isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String dblpInputPath = parser.get("dblpInputPath");
		log.info("dblpInputPath: {}", dblpInputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);

//					TODO: add DBLP ID in ModelConstants
				List<KeyValue> collectedFromDBLP = OafMapperUtils
					.listKeyValues(ModelConstants.CROSSREF_ID, "DBLP");
				JavaPairRDD<Text, Text> dblpData = prepareDblpData(
					spark, dblpInputPath, collectedFromDBLP);

				dblpData
					.saveAsHadoopFile(
						outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);

			});
	}

	private static <I extends Result> JavaPairRDD<Text, Text> prepareDblpData(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedFrom) {

//		TODO: load DBLP data into a Dataset
		Dataset<Row> df = spark
			.read()
			.schema("`DOI` STRING, `Matchings` ARRAY<STRUCT<`RORid`:STRING,`Confidence`:DOUBLE>>")
			.json(inputPath);

		return df.map((MapFunction<Row, Result>) bs -> {
			Result result = new Result();

//					TODO: map DBLP data to Result objects

			return result;

		}, Encoders.bean(Result.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(Result.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))));
	}
}
