
package eu.dnetlib.dhp.oa.graph.clean;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
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
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

/**
 * @author miriam.baglioni
 * @Date 22/07/22
 */
public class GetDatasourceFromCountry implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(GetDatasourceFromCountry.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				GetDatasourceFromCountry.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/input_datasource_country_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String workingPath = parser.get("workingDir");
		log.info("workingDir: {}", workingPath);

		String country = parser.get("country");
		log.info("country: {}", country);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				getDatasourceFromCountry(spark, country, inputPath, workingPath);
			});
	}

	private static void getDatasourceFromCountry(SparkSession spark, String country, String inputPath,
		String workingDir) {

		Dataset<Organization> organization = spark
			.read()
			.textFile(inputPath + "/organization")
			.map(
				(MapFunction<String, Organization>) value -> OBJECT_MAPPER.readValue(value, Organization.class),
				Encoders.bean(Organization.class))
			.filter(
				(FilterFunction<Organization>) o -> !o.getDataInfo().getDeletedbyinference() &&
					o.getCountry() != null &&
					o.getCountry().getClassid() != null &&
					o.getCountry().getClassid().length() > 0 &&
					o.getCountry().getClassid().equals(country));

		// filtering of the relations taking the non deleted by inference and those with IsProvidedBy as relclass
		Dataset<Relation> relation = spark
			.read()
			.textFile(inputPath + "/relation")
			.map(
				(MapFunction<String, Relation>) value -> OBJECT_MAPPER.readValue(value, Relation.class),
				Encoders.bean(Relation.class))
			.filter(
				(FilterFunction<Relation>) rel -> rel.getRelClass().equalsIgnoreCase(ModelConstants.IS_PROVIDED_BY) &&
					!rel.getDataInfo().getDeletedbyinference());

		organization
			.joinWith(relation, organization.col("id").equalTo(relation.col("target")))
			.map((MapFunction<Tuple2<Organization, Relation>, String>) t2 -> t2._2().getSource(), Encoders.STRING())
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir);

	}
}
