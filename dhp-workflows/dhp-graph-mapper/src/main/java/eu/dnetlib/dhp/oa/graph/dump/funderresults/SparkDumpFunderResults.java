
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.community.Project;
import scala.Tuple2;

/**
 * Splits the dumped results by funder and stores them in a folder named as the funder nsp (for all the funders, but the EC
 * for the EC it specifies also the fundingStream (FP7 or H2020)
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

		final String graphPath = parser.get("graphPath");
		log.info("relationPath: {}", graphPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				writeResultProjectList(spark, inputPath, outputPath, graphPath);
			});
	}

	private static void writeResultProjectList(SparkSession spark, String inputPath, String outputPath,
		String graphPath) {

		Dataset<String> funderList = Utils
				.readPath(spark, inputPath + "/publication", CommunityResult.class)
				.union(Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class))
				.union(Utils.readPath(spark, inputPath + "/otherresearchproduct", CommunityResult.class))
				.union(Utils.readPath(spark, inputPath + "/software", CommunityResult.class))
				.flatMap((FlatMapFunction<CommunityResult, String>) cr ->
								cr.getProjects().stream().map(p -> p.getFunder().getShortName()).collect(Collectors.toList()).iterator()
						, Encoders.STRING())
				.distinct();

		Dataset<CommunityResult> pubs;
		Dataset<CommunityResult> result ;
		pubs = Utils
				.readPath(spark, inputPath + "/publication", CommunityResult.class);
		Dataset<CommunityResult> dats = Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class);
		Dataset<CommunityResult> orp = Utils.readPath(spark, inputPath + "/otherresearchproduct", CommunityResult.class);
		Dataset<CommunityResult> sw = Utils.readPath(spark, inputPath + "/software", CommunityResult.class);
		result = pubs.union(dats).union(orp).union(sw);

		funderList.foreach((ForeachFunction<String>) funder ->
				getFunderResult(funder, inputPath, spark)
				.write()
							.mode(SaveMode.Overwrite)
							.option("compression", "gzip")
							.json(outputPath + "/" + funder)

		);


	}


	@Nullable
	private static Dataset<CommunityResult> getFunderResult(String funderName, String inputPath, SparkSession spark) {
		Dataset<CommunityResult> pubs;
		Dataset<CommunityResult> result ;
				pubs = Utils
				.readPath(spark, inputPath + "/publication", CommunityResult.class);
		Dataset<CommunityResult> dats = Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class);
		Dataset<CommunityResult> orp = Utils.readPath(spark, inputPath + "/otherresearchproduct", CommunityResult.class);
		Dataset<CommunityResult> sw = Utils.readPath(spark, inputPath + "/software", CommunityResult.class);
		result = pubs.union(dats).union(orp).union(sw);
		Dataset<CommunityResult> tmp = result.map((MapFunction<CommunityResult, CommunityResult>) cr -> {
					if (!Optional.ofNullable(cr.getProjects()).isPresent()) {
						return null;
					}
					for (Project p : cr.getProjects()) {
						if (p.getFunder().getShortName().equalsIgnoreCase(funderName)) {
							return cr;
						}
					}
					return null;
				}, Encoders.bean(CommunityResult.class))
				.filter(Objects::nonNull);
		System.out.println(tmp.count());
		return tmp;

	}


}
