
package eu.dnetlib.dhp.entitytoorganizationfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttoorganizationfrominstrepo.SparkResultToOrganizationFromIstRepoJob;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

/**
 * Searches for all the association between result and organization already existing in the graph
 * Creates also the parenthood hierarchy from the organizations
 */

public class PrepareInfo implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareInfo.class);

	// associate orgs with all their parent
	private static final String ORGANIZATION_ORGANIZATION_QUERY = "SELECT target key, collect_set(source) as valueSet "
		+
		"FROM relation " +
		"WHERE lower(relclass) = '" + ModelConstants.IS_PARENT_OF.toLowerCase() +
		"' and datainfo.deletedbyinference = false " +
		"GROUP BY target";

	// associates results with all the orgs they are affiliated to
	private static final String RESULT_ORGANIZATION_QUERY = "SELECT source key, collect_set(target) as valueSet " +
		"FROM relation " +
		"WHERE lower(relclass) = '" + ModelConstants.HAS_AUTHOR_INSTITUTION.toLowerCase() +
		"' and datainfo.deletedbyinference = false " +
		"GROUP BY source";

	// associate projects to all the participant orgs
	private static final String PROJECT_ORGANIZATION_QUERY = "SELECT source key, collect_set(target) as valueSet " +
		"FROM relation " +
		"WHERE lower(relclass) = '" + ModelConstants.HAS_PARTICIPANT.toLowerCase() +
		"' and datainfo.deletedbyinference = false " +
		"GROUP BY source";

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkResultToOrganizationFromIstRepoJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/entitytoorganizationfromsemrel/input_preparation_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String leavesPath = parser.get("leavesPath");
		log.info("leavesPath: {}", leavesPath);

		final String childParentPath = parser.get("childParentPath");
		log.info("childParentPath: {}", childParentPath);

		final String resultOrganizationPath = parser.get("resultOrgPath");
		log.info("resultOrganizationPath: {}", resultOrganizationPath);

		final String projectOrgPath = parser.get("projectOrganizationPath");
		log.info("projectOrgPath: {}", projectOrgPath);

		final String relationPath = parser.get("relationPath");
		log.info("relationPath: {}", relationPath);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> prepareInfo(
				spark,
				graphPath,
				childParentPath,
				leavesPath,
				resultOrganizationPath,
				projectOrgPath,
				relationPath));
	}

	private static void prepareInfo(SparkSession spark, String inputPath, String childParentOrganizationPath,
		String currentIterationPath, String resultOrganizationPath, String projectOrganizationPath,
		String relationPath) {
		Dataset<Relation> relation = readPath(spark, inputPath + "/relation", Relation.class);
		relation.createOrReplaceTempView("relation");

		spark
			.sql(ORGANIZATION_ORGANIZATION_QUERY)
			.as(Encoders.bean(KeyValueSet.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(childParentOrganizationPath);

		spark
			.sql(RESULT_ORGANIZATION_QUERY)
			.as(Encoders.bean(KeyValueSet.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(resultOrganizationPath);

		spark
			.sql(PROJECT_ORGANIZATION_QUERY)
			.as(Encoders.bean(KeyValueSet.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(projectOrganizationPath);

		relation
			.filter(
				(FilterFunction<Relation>) r -> !r.getDataInfo().getDeletedbyinference() &&
					r.getRelClass().equals(ModelConstants.HAS_AUTHOR_INSTITUTION))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(relationPath + "/result");

		relation
			.filter(
				(FilterFunction<Relation>) r -> !r.getDataInfo().getDeletedbyinference() &&
					r.getRelClass().equals(ModelConstants.HAS_PARTICIPANT))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(relationPath + "/project");

		Dataset<String> children = spark
			.sql(
				"Select distinct target as child from relation where " +
					"lower(relclass)='" + ModelConstants.IS_PARENT_OF.toLowerCase() +
					"' and datainfo.deletedbyinference = false")
			.as(Encoders.STRING());

		Dataset<String> parent = spark
			.sql(
				"Select distinct source as parent from relation " +
					"where lower(relclass)='" + ModelConstants.IS_PARENT_OF.toLowerCase() +
					"' and datainfo.deletedbyinference = false")
			.as(Encoders.STRING());

		// takes from the join the entities having only the left hand side: the leaves. Saves them
		children
			.joinWith(parent, children.col("child").equalTo(parent.col("parent")), "left")
			.map((MapFunction<Tuple2<String, String>, String>) value -> {
				if (Optional.ofNullable(value._2()).isPresent()) {
					return null;
				}

				return value._1();
			}, Encoders.STRING())
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.json(currentIterationPath);
	}

}
