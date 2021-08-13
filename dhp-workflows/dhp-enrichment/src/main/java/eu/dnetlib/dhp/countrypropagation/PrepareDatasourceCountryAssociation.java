
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * For the association of the country to the datasource The association is computed only for datasource of specific type
 * or having whitelisted ids The country is registered in the Organization associated to the Datasource, so the relation
 * provides between Datasource and Organization is exploited to get the country for the datasource
 */
public class PrepareDatasourceCountryAssociation {

	private static final Logger log = LoggerFactory.getLogger(PrepareDatasourceCountryAssociation.class);

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

		final String whitelisted = whitelist
			.stream()
			.map(id -> " d.id = '" + id + "'")
			.collect(Collectors.joining(" OR "));

		final String allowed = allowedtypes
			.stream()
			.map(type -> " d.datasourcetype.classid = '" + type + "'")
			.collect(Collectors.joining(" OR "));

		Dataset<Datasource> datasource = readPath(spark, inputPath + "/datasource", Datasource.class);
		Dataset<Relation> relation = readPath(spark, inputPath + "/relation", Relation.class);
		Dataset<Organization> organization = readPath(spark, inputPath + "/organization", Organization.class);

		datasource.createOrReplaceTempView("datasource");
		relation.createOrReplaceTempView("relation");
		organization.createOrReplaceTempView("organization");

		String query = "SELECT source dataSourceId, " +
			"named_struct('classid', country.classid, 'classname', country.classname) country " +
			"FROM datasource d " +
			"JOIN relation rel " +
			"ON d.id = rel.source " +
			"JOIN organization o " +
			"ON o.id = rel.target " +
			"WHERE rel.datainfo.deletedbyinference = false  " +
			"and lower(rel.relclass) = '" + ModelConstants.IS_PROVIDED_BY.toLowerCase() + "'" +
			"and o.datainfo.deletedbyinference = false  " +
			"and length(o.country.classid) > 0 " +
			"and (" + allowed + " or " + whitelisted + ")";

		spark
			.sql(query)
			.as(Encoders.bean(DatasourceCountry.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);
	}
}
