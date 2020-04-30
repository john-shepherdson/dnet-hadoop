
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;

/**
 * For the association of the country to the datasource The association is computed only for datasource of specific type
 * or having whitelisted ids The country is registered in the Organization associated to the Datasource, so the relation
 * provides between Datasource and Organization is exploited to get the country for the datasource
 */
public class PrepareDatasourceCountryAssociation {

	private static final Logger log = LoggerFactory.getLogger(PrepareDatasourceCountryAssociation.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareDatasourceCountryAssociation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/countrypropagation/input_prepareassoc_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareDatasourceCountryAssociation(
					spark,
					Arrays.asList(parser.get("whitelist").split(";")),
					Arrays.asList(parser.get("allowedtypes").split(";")),
					inputPath,
					outputPath);
			});
	}

	private static void prepareDatasourceCountryAssociation(
		SparkSession spark,
		List<String> whitelist,
		List<String> allowedtypes,
		String inputPath,
		String outputPath) {
		String whitelisted = "";
		for (String i : whitelist) {
			whitelisted += " OR id = '" + i + "'";
		}
		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		Dataset<Datasource> datasource = spark
			.createDataset(
				sc
					.textFile(inputPath + "/datasource")
					.map(item -> OBJECT_MAPPER.readValue(item, Datasource.class))
					.rdd(),
				Encoders.bean(Datasource.class));

		Dataset<Relation> relation = spark
			.createDataset(
				sc
					.textFile(inputPath + "/relation")
					.map(item -> OBJECT_MAPPER.readValue(item, Relation.class))
					.rdd(),
				Encoders.bean(Relation.class));

		Dataset<Organization> organization = spark
			.createDataset(
				sc
					.textFile(inputPath + "/organization")
					.map(item -> OBJECT_MAPPER.readValue(item, Organization.class))
					.rdd(),
				Encoders.bean(Organization.class));

		datasource.createOrReplaceTempView("datasource");
		relation.createOrReplaceTempView("relation");
		organization.createOrReplaceTempView("organization");

		String query = "SELECT source dataSourceId, named_struct('classid', country.classid, 'classname', country.classname) country "
			+ "FROM ( SELECT id "
			+ "       FROM datasource "
			+ "       WHERE (datainfo.deletedbyinference = false "
			+ whitelisted
			+ ") "
			+ getConstraintList("datasourcetype.classid = '", allowedtypes)
			+ ") d "
			+ "JOIN ( SELECT source, target "
			+ "       FROM relation "
			+ "       WHERE relclass = '"
			+ RELATION_DATASOURCE_ORGANIZATION_REL_CLASS
			+ "' "
			+ "       AND datainfo.deletedbyinference = false ) rel "
			+ "ON d.id = rel.source "
			+ "JOIN (SELECT id, country "
			+ "      FROM organization "
			+ "      WHERE datainfo.deletedbyinference = false "
			+ "      AND length(country.classid)>0) o "
			+ "ON o.id = rel.target";

		spark
			.sql(query)
			.as(Encoders.bean(DatasourceCountry.class))
			.toJavaRDD()
			.map(c -> OBJECT_MAPPER.writeValueAsString(c))
			.saveAsTextFile(outputPath, GzipCodec.class);
	}
}
