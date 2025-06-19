
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static eu.dnetlib.dhp.actionmanager.bipaffiliations.Constants.OALEX_SCHEMA;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.io.Serializable;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.DHPUtils;

//It prepares the dataset made from the various inputs to be used by affro
//inputs: oalex from /data/openalex-snapshot
//input oaire from a previous execution of the pipeline
//input publishers files
//input iis table that stores pdf extraction data
public class PrepareDataset implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareDataset.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________::";
	public static final String BIP_AFFILIATIONS_CLASSID = "result:organization:openaireinference";
	public static final String BIP_AFFILIATIONS_CLASSNAME = "Affiliation relation inferred by OpenAIRE";
	public static final String BIP_INFERENCE_PROVENANCE = "openaire:affiliation";
	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";
	public static final String DOI_URL_PREFIX = "https://doi.org/";
	public static final int DOI_URL_PREFIX_LENGTH = 16;
	private static final Object OPENORGS_NS_PREFIX = "openorgs____";

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareDataset.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/bipaffiliations/input_preparedataset_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Constants.isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String oalexPath = parser.get("oalexPath");
		log.info("oalexPath: {}", oalexPath);

		final String oairePath = parser.get("oairePath");
		log.info("oairePath: {}", oairePath);

		final String publishersPath = parser.get("publishersPath");
		log.info("publishersPath: {}", publishersPath);

		final String iisPath = parser.get("iisPath");
		log.info("iisPath: {}", iisPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);
				prepareDataset(
					spark, oalexPath, oairePath, iisPath, publishersPath, outputPath);
			});
	}

	private static void prepareDataset(SparkSession spark, String oalexPath, String oairePath, String iisPath,
		String publishersPath, String outputPath) {
		// start with oalex. read from the snapshot in the schema needed for this task
		// Function to compute MD5 hash with prefix
		spark
			.udf()
			.register(
				"md5HashWithPrefix", (String doi) -> "50|doi_________::" + DHPUtils.md5(doi), DataTypes.StringType);

		Dataset<Row> oalex = spark
			.read()
			.schema(OALEX_SCHEMA)
			.json(oalexPath)
			.filter(functions.col("doi").isNotNull())
			.withColumn("id", functions.expr("md5HashWihPrefix(doi)"))
			.withColumn(
				"authors",
				functions
					.expr(
						"transform(authorships, x -> struct(x.author.display_name as fullname, x.raw_affiliation_strings))"));

	}

}
