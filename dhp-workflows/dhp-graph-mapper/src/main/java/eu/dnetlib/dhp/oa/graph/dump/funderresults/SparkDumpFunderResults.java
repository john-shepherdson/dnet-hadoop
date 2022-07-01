
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
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.community.Funder;
import eu.dnetlib.dhp.schema.dump.oaf.community.Project;

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
		log.info("Number of result {}", result.count());
		Dataset<String> tmp = result
			.flatMap((FlatMapFunction<CommunityResult, String>) cr -> cr.getProjects().stream().map(p -> {
				return getFunderName(p);
			}).collect(Collectors.toList()).iterator(), Encoders.STRING())
			.distinct();
		List<String> funderList = tmp.collectAsList();
		funderList.forEach(funder -> {
			dumpResults(funder, result, outputPath);
		});
	}

	@NotNull
	private static String getFunderName(Project p) {
		Optional<Funder> ofunder = Optional.ofNullable(p.getFunder());
		if (ofunder.isPresent()) {
			String fName = ofunder.get().getShortName();
			if (fName.equalsIgnoreCase("ec")) {
				fName += "_" + ofunder.get().getFundingStream();
			}
			return fName;
		} else {
			String fName = p.getId().substring(3, p.getId().indexOf("_")).toUpperCase();
			if (fName.equalsIgnoreCase("ec")) {
				if (p.getId().contains("h2020")) {
					fName += "_H2020";
				} else {
					fName += "_FP7";
				}
			} else if (fName.equalsIgnoreCase("conicytf")) {
				fName = "CONICYT";
			} else if (fName.equalsIgnoreCase("dfgf")) {
				fName = "DFG";
			} else if (fName.equalsIgnoreCase("tubitakf")) {
				fName = "TUBITAK";
			} else if (fName.equalsIgnoreCase("euenvagency")) {
				fName = "EEA";
			}
			return fName;
		}
	}

	private static void dumpResults(String funder, Dataset<CommunityResult> results, String outputPath) {
		results.map((MapFunction<CommunityResult, CommunityResult>) r -> {
			if (!Optional.ofNullable(r.getProjects()).isPresent()) {
				return null;
			}
			for (Project p : r.getProjects()) {
				String fName = getFunderName(p);
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
