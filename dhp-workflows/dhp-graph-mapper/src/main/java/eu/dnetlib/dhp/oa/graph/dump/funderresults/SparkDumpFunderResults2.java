
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.community.Project;

/**
 * Splits the dumped results by funder and stores them in a folder named as the funder nsp (for all the funders, but the EC
 * for the EC it specifies also the fundingStream (FP7 or H2020)
 */
public class SparkDumpFunderResults2 implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkDumpFunderResults2.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpFunderResults2.class
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
				writeResultProjectList(spark, inputPath, outputPath);
			});
	}

	private static void writeResultProjectList(SparkSession spark, String inputPath, String outputPath) {

		Dataset<CommunityResult> result = Utils
			.readPath(spark, inputPath + "/publication", CommunityResult.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/otherresearchproduct", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/software", CommunityResult.class));

		List<String> funderList = result
			.flatMap((FlatMapFunction<CommunityResult, String>) cr -> cr.getProjects().stream().map(p -> {
				String fName = p.getFunder().getShortName();
				if (fName.equalsIgnoreCase("ec")) {
					fName += "_" + p.getFunder().getFundingStream();
				}
				return fName;
			}).collect(Collectors.toList()).iterator(), Encoders.STRING())
			.distinct()
			.collectAsList();

		funderList.forEach(funder -> {

			dumpResults(funder, result, outputPath);
		});

	}

	private static void dumpResults(String funder, Dataset<CommunityResult> results, String outputPath) {

		results.map((MapFunction<CommunityResult, CommunityResult>) r -> {
			if (!Optional.ofNullable(r.getProjects()).isPresent()) {
				return null;
			}
			for (Project p : r.getProjects()) {
				String fName = p.getFunder().getShortName();
				if (fName.equalsIgnoreCase("ec")) {
					fName += "_" + p.getFunder().getFundingStream();
				}
				if (fName.equalsIgnoreCase(funder)) {
					return r;
				}
			}
			return null;
		}, Encoders.bean(CommunityResult.class))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/" + funder);
	}

}
