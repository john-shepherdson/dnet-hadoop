
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.ResultMapper;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

/**
 * Preparation of the Project information to be added to the dumped results. For each result associated to at least one
 * Project, a serialization of an instance af ResultProject closs is done. ResultProject contains the resultId, and the
 * list of Projects (as in eu.dnetlib.dhp.schema.dump.oaf.community.Project) it is associated to
 */
public class SparkDumpFunderResults implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkDumpFunderResults.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpFunderResults.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/funder_result_parameters.json"));

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

		final String communityMapPath = parser.get("communityMapPath");
		log.info("communityMapPath: {}", communityMapPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				writeResultProjectList(spark, inputPath, outputPath, communityMapPath);
			});
	}

	private static void writeResultProjectList(SparkSession spark, String inputPath, String outputPath,
											   String communityMapPath) {

		CommunityMap communityMap = Utils.getCommunityMap(spark, communityMapPath);

		Dataset<Relation> relation = Utils
			.readPath(spark, inputPath + "/relation", Relation.class)
			.filter("dataInfo.deletedbyinference = false and relClass = 'produces'");

		Dataset<eu.dnetlib.dhp.schema.oaf.Result> result = Utils
			.readPath(spark, inputPath + "/publication", eu.dnetlib.dhp.schema.oaf.Result.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Result.class))
			.union(Utils.readPath(spark, inputPath + "/otherresearchproduct", eu.dnetlib.dhp.schema.oaf.Result.class))
			.union(Utils.readPath(spark, inputPath + "/software", eu.dnetlib.dhp.schema.oaf.Result.class));

		result
			.joinWith(relation, result.col("id").equalTo(relation.col("target")), "inner")
				.map((MapFunction<Tuple2<eu.dnetlib.dhp.schema.oaf.Result, Relation>, FunderResults>) value ->{
					FunderResults res = (FunderResults) ResultMapper.map(value._1(), communityMap, false);
					res.setFunder_id(value._2().getSource().substring(3,15));
					return res;
				}, Encoders.bean(FunderResults.class))
				.write()
				.partitionBy("funder_id")
				.mode(SaveMode.Overwrite)
				.json(outputPath);

	}

}
