
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
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
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

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
						"/eu/dnetlib/dhp/wf/subworkflows/countrypropagation/input_prepareassoc_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				// removeOutputDir(spark, outputPath);
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

		// filtering of the datasource taking only the non deleted by inference and those with the allowed types or
		// whose id is in whitelist
		Dataset<Datasource> datasource = readPath(spark, inputPath + "/datasource", Datasource.class)
			.filter(
				(FilterFunction<Datasource>) ds -> !ds.getDataInfo().getDeletedbyinference() &&
					Optional.ofNullable(ds.getDatasourcetype()).isPresent() &&
					Optional.ofNullable(ds.getDatasourcetype().getClassid()).isPresent() &&
					((Optional.ofNullable(ds.getJurisdiction()).isPresent() &&
						allowedtypes.contains(ds.getJurisdiction().getClassid())) ||
						whitelist.contains(ds.getId())));

		// filtering of the relations taking the non deleted by inference and those with IsProvidedBy as relclass
		Dataset<Relation> relation = readPath(spark, inputPath + "/relation", Relation.class)
			.filter(
				(FilterFunction<Relation>) rel -> rel.getRelClass().equalsIgnoreCase(ModelConstants.IS_PROVIDED_BY) &&
					!rel.getDataInfo().getDeletedbyinference());

		// filtering of the organization taking only the non deleted by inference and those with information about the
		// country
		Dataset<Organization> organization = readPath(spark, inputPath + "/organization", Organization.class)
			.filter(
				(FilterFunction<Organization>) o -> !o.getDataInfo().getDeletedbyinference() &&
					o.getCountry().getClassid().length() > 0 &&
					!o.getCountry().getClassid().equals(ModelConstants.UNKNOWN));

		// associated the datasource id with the id of the organization providing the datasource
		Dataset<EntityEntityRel> dse = datasource
			.joinWith(relation, datasource.col("id").equalTo(relation.col("source")))
			.map(
				(MapFunction<Tuple2<Datasource, Relation>, EntityEntityRel>) t2 -> EntityEntityRel
					.newInstance(t2._2.getSource(), t2._2.getTarget()),
				Encoders.bean(EntityEntityRel.class));

		// joins with the information stored in the organization dataset to associate the country to the datasource id
		dse
			.joinWith(organization, dse.col("entity2Id").equalTo(organization.col("id")))
			.map((MapFunction<Tuple2<EntityEntityRel, Organization>, DatasourceCountry>) t2 -> {
				Qualifier country = t2._2.getCountry();
				return DatasourceCountry
					.newInstance(
						t2._1.getEntity1Id(),
						CountrySbs.newInstance(country.getClassid(), country.getClassname()));
			}, Encoders.bean(DatasourceCountry.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);
	}
}
