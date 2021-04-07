
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.oaf.*;

/**
 * It selects the valid relations among those present in the graph. One relation is valid if it is not deletedbyinference
 * and if both the source and the target node are present in the graph and are not deleted by inference nor invisible.
 * To check this I made a view of the ids of all the entities in the graph, and select the relations for which a join exists
 * with this view for both the source and the target
 */

public class SparkSelectValidRelationsJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkSelectValidRelationsJob.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkSelectValidRelationsJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/complete/input_relationdump_parameters.json"));

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

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				selectValidRelation(spark, inputPath, outputPath);

			});

	}

	private static void selectValidRelation(SparkSession spark, String inputPath, String outputPath) {
		Dataset<Relation> relation = Utils.readPath(spark, inputPath + "/relation", Relation.class);
		Dataset<Publication> publication = Utils.readPath(spark, inputPath + "/publication", Publication.class);
		Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> dataset = Utils
			.readPath(spark, inputPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class);
		Dataset<Software> software = Utils.readPath(spark, inputPath + "/software", Software.class);
		Dataset<OtherResearchProduct> other = Utils
			.readPath(spark, inputPath + "/otherresearchproduct", OtherResearchProduct.class);
		Dataset<Organization> organization = Utils.readPath(spark, inputPath + "/organization", Organization.class);
		Dataset<Project> project = Utils.readPath(spark, inputPath + "/project", Project.class);
		Dataset<Datasource> datasource = Utils.readPath(spark, inputPath + "/datasource", Datasource.class);

		relation.createOrReplaceTempView("relation");
		publication.createOrReplaceTempView("publication");
		dataset.createOrReplaceTempView("dataset");
		other.createOrReplaceTempView("other");
		software.createOrReplaceTempView("software");
		organization.createOrReplaceTempView("organization");
		project.createOrReplaceTempView("project");
		datasource.createOrReplaceTempView("datasource");

		spark
			.sql(
				"SELECT id " +
					"FROM publication " +
					"WHERE datainfo.deletedbyinference = false AND  datainfo.invisible = false " +
					"UNION ALL " +
					"SELECT id " +
					"FROM dataset " +
					"WHERE datainfo.deletedbyinference = false AND  datainfo.invisible = false " +
					"UNION ALL " +
					"SELECT id " +
					"FROM other " +
					"WHERE datainfo.deletedbyinference = false AND  datainfo.invisible = false " +
					"UNION ALL " +
					"SELECT id " +
					"FROM software " +
					"WHERE datainfo.deletedbyinference = false AND  datainfo.invisible = false " +
					"UNION ALL " +
					"SELECT id " +
					"FROM organization " +
					"WHERE datainfo.deletedbyinference = false AND  datainfo.invisible = false " +
					"UNION ALL " +
					"SELECT id " +
					"FROM project " +
						"WHERE datainfo.deletedbyinference = false AND  datainfo.invisible = false " +
					"UNION ALL " +
					"SELECT id " +
					"FROM datasource " +
						"WHERE datainfo.deletedbyinference = false AND  datainfo.invisible = false " )
			.createOrReplaceTempView("identifiers");

		spark
			.sql(
				"SELECT relation.* " +
					"FROM relation " +
					"JOIN identifiers i1 " +
					"ON source = i1.id " +
					"JOIN identifiers i2 " +
					"ON target = i2.id " +
					"WHERE datainfo.deletedbyinference = false")
			.as(Encoders.bean(Relation.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);
		;

	}
}
