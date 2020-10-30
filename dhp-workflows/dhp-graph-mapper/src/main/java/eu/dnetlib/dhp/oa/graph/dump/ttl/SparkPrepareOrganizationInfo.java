
package eu.dnetlib.dhp.oa.graph.dump.ttl;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.oaf.Organization;
import scala.Tuple2;

public class SparkPrepareOrganizationInfo implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkPrepareOrganizationInfo.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkPrepareOrganizationInfo.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_whole/input_organization_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				prepareOrganization(spark, inputPath, outputPath);

			});

	}

	private static void prepareOrganization(SparkSession spark, String inputPath, String outputPath) {
		Dataset<Organization> orgs = Utils.readPath(spark, inputPath, Organization.class);

		orgs.createOrReplaceTempView("organization");

		String query = "select country.classname country, legalname.value name, legalshortname.value shortName, websiteurl.value websiteUrl, "
			+
			"collect_set(named_struct(\"pidtype\", pIde.qualifier.classid, \"pid\", pIde.value)) as pidsList, id " +
			"from organization " +
			"lateral view explode( pid) p as pIde " +
			"group by country.classname, legalname.value, legalshortname.value, websiteurl.value, id";

		spark
			.sql(query)
			.as(Encoders.bean(OrganizationInfo.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

}
