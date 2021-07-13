
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;
import org.apache.spark.sql.*;
import eu.dnetlib.dhp.schema.oaf.Project;
/**
 * Selects the results linked to projects. Only for these results the dump will be performed.
 * The code to perform the dump and to expend the dumped results with the informaiton related to projects
 * is the one used for the dump of the community products
 */
public class SparkResultLinkedToProject implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkResultLinkedToProject.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkResultLinkedToProject.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_parameters_link_prj.json"));

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

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);
		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				writeResultsLinkedToProjects(spark, inputClazz, inputPath, outputPath, graphPath);
			});
	}

	private static <R extends Result> void writeResultsLinkedToProjects(SparkSession spark, Class<R> inputClazz,
		String inputPath, String outputPath, String graphPath) {

		Dataset<R> results = Utils
			.readPath(spark, inputPath, inputClazz)
			.filter("dataInfo.deletedbyinference = false and datainfo.invisible = false");
		Dataset<Relation> relations = Utils
			.readPath(spark, graphPath + "/relation", Relation.class)
			.filter(
				"dataInfo.deletedbyinference = false and lower(relClass) = '"
					+ ModelConstants.IS_PRODUCED_BY.toLowerCase() + "'");

		spark
				.sql(
						"Select res.* " +
								"from relation rel " +
								"join result res " +
								"on rel.source = res.id " +
								"join project p " +
								"on rel.target = p.id " +
								"")
				.as(Encoders.bean(inputClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}
}
