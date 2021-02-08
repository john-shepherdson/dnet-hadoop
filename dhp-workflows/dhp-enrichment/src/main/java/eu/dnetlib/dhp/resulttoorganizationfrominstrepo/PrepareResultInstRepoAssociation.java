
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
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
import eu.dnetlib.dhp.schema.oaf.Relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class PrepareResultInstRepoAssociation {

	private static final Logger log = LoggerFactory.getLogger(PrepareResultInstRepoAssociation.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultInstRepoAssociation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_prepareresultorg_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String datasourceOrganizationPath = parser.get("datasourceOrganizationPath");
		log.info("datasourceOrganizationPath {}: ", datasourceOrganizationPath);

		final String alreadyLinkedPath = parser.get("alreadyLinkedPath");
		log.info("alreadyLinkedPath {}: ", alreadyLinkedPath);

		List<String> blacklist = Optional.ofNullable(parser.get("blacklist"))
				.map(v -> Arrays.asList(v.split(";")))
				.orElse(new ArrayList<>());

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				readNeededResources(spark, inputPath);

				removeOutputDir(spark, datasourceOrganizationPath);
				prepareDatasourceOrganization(spark, datasourceOrganizationPath, blacklist);

				removeOutputDir(spark, alreadyLinkedPath);
				prepareAlreadyLinkedAssociation(spark, alreadyLinkedPath);
			});
	}

	private static void readNeededResources(SparkSession spark, String inputPath) {
		Dataset<Datasource> datasource = readPath(spark, inputPath + "/datasource", Datasource.class);
		datasource.createOrReplaceTempView("datasource");

		Dataset<Relation> relation = readPath(spark, inputPath + "/relation", Relation.class);
		relation.createOrReplaceTempView("relation");

		Dataset<Organization> organization = readPath(spark, inputPath + "/organization", Organization.class);
		organization.createOrReplaceTempView("organization");
	}

	private static void prepareDatasourceOrganization(
		SparkSession spark, String datasourceOrganizationPath, List<String> blacklist) {
		String blacklisted = "";
		if(blacklist.size() > 0 ){
			blacklisted = " AND  d.id != '" + blacklist.get(0) + "'";
			for (int i = 1; i < blacklist.size(); i++) {
				blacklisted += " AND d.id != '" + blacklist.get(i) + "'";
			}
		}


		String query = "SELECT source datasourceId, target organizationId "
			+ "FROM ( SELECT id "
			+ "FROM datasource "
			+ "WHERE datasourcetype.classid = '"
			+ INSTITUTIONAL_REPO_TYPE
			+ "' "
			+ "AND datainfo.deletedbyinference = false  " + blacklisted + " ) d "
			+ "JOIN ( SELECT source, target "
			+ "FROM relation "
			+ "WHERE lower(relclass) = '"
			+ ModelConstants.IS_PROVIDED_BY.toLowerCase()
			+ "' "
			+ "AND datainfo.deletedbyinference = false ) rel "
			+ "ON d.id = rel.source ";

		spark
			.sql(query)
			.as(Encoders.bean(DatasourceOrganization.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(datasourceOrganizationPath);
	}

	private static void prepareAlreadyLinkedAssociation(
		SparkSession spark, String alreadyLinkedPath) {
		String query = "Select source resultId, collect_set(target) organizationSet "
			+ "from relation "
			+ "where datainfo.deletedbyinference = false "
			+ "and lower(relClass) = '"
			+ ModelConstants.HAS_AUTHOR_INSTITUTION.toLowerCase()
			+ "' "
			+ "group by source";

		spark
			.sql(query)
			.as(Encoders.bean(ResultOrganizationSet.class))
			// TODO retry to stick with datasets
			.toJavaRDD()
			.map(r -> OBJECT_MAPPER.writeValueAsString(r))
			.saveAsTextFile(alreadyLinkedPath, GzipCodec.class);
	}

}
