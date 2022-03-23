
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.oa.graph.dump.ResultMapper;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.oa.graph.dump.community.ResultProject;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;

/**
 * Selects the results linked to projects. Only for these results the dump will be performed.
 * The code to perform the dump and to expend the dumped results with the information related to projects
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

		final String resultProjectsPath = parser.get("graphPath");
		log.info("graphPath: {}", resultProjectsPath);

		String communityMapPath = parser.get("communityMapPath");

		@SuppressWarnings("unchecked")
		Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);
		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				writeResultsLinkedToProjects(communityMapPath, spark, inputClazz, inputPath, outputPath, resultProjectsPath);
			});
	}

	private static <R extends Result> void writeResultsLinkedToProjects(String communityMapPath, SparkSession spark, Class<R> inputClazz,
		String inputPath, String outputPath, String resultProjectsPath) {

		Dataset<R> results = Utils
			.readPath(spark, inputPath, inputClazz)
				.filter((FilterFunction<R>) r -> !r.getDataInfo().getDeletedbyinference() &&
						!r.getDataInfo().getInvisible())
			;
		Dataset<ResultProject> resultProjectDataset = Utils
			.readPath(spark, resultProjectsPath , ResultProject.class)
			;
		CommunityMap communityMap = Utils.getCommunityMap(spark, communityMapPath);
		results.joinWith(resultProjectDataset, results.col("id").equalTo(resultProjectDataset.col("resultId")))
				.map((MapFunction<Tuple2<R, ResultProject>, CommunityResult>) t2 ->
						{
							CommunityResult cr = (CommunityResult) ResultMapper.map(t2._1(),
									communityMap, Constants.DUMPTYPE.FUNDER.getType());
							cr.setProjects(t2._2().getProjectsList());
							return cr;
						}

						, Encoders.bean(CommunityResult.class) )
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

	}
}
