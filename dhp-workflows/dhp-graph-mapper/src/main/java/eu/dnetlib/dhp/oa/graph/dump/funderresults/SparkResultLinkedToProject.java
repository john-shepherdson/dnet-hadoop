
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;

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
		Dataset<Project> project = Utils.readPath(spark, graphPath + "/project", Project.class);

		results.createOrReplaceTempView("result");
		relations.createOrReplaceTempView("relation");
		project.createOrReplaceTempView("project");

		Dataset<R> tmp = spark
			.sql(
				"Select res.* " +
					"from relation rel " +
					"join result res " +
					"on rel.source = res.id " +
					"join project p " +
					"on rel.target = p.id " +
						"")
			.as(Encoders.bean(inputClazz))
				;
		tmp.groupByKey(
				(MapFunction< R, String>) value -> value
					.getId(),
				Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, R, R>) (k, it) -> it.next(), Encoders.bean(inputClazz))

//
//		relations
//			.joinWith(
//				results, relations.col("source").equalTo(results.col("id")),
//				"inner")
//			.groupByKey(
//				(MapFunction<Tuple2<Relation, R>, String>) value -> value
//					._2()
//					.getId(),
//				Encoders.STRING())
//			.mapGroups((MapGroupsFunction<String, Tuple2<Relation, R>, R>) (k, it) -> {
//				return it.next()._2();
//			}, Encoders.bean(inputClazz))
		//tmp
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}
}
