
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.api.zenodo.Community;
import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.ResultMapper;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.community.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
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

		final String relationPath = parser.get("relationPath");
		log.info("relationPath: {}", relationPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				writeResultProjectList(spark, inputPath, outputPath, relationPath);
			});
	}

	private static void writeResultProjectList(SparkSession spark, String inputPath, String outputPath,
		String relationPath) {

		Dataset<Relation> relation = Utils
			.readPath(spark, relationPath + "/relation", Relation.class)
			.filter(
				"dataInfo.deletedbyinference = false and lower(relClass) = '"
					+ ModelConstants.IS_PRODUCED_BY.toLowerCase() + "'");

		Dataset<CommunityResult> result = Utils
			.readPath(spark, inputPath + "/publication", CommunityResult.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/orp", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/software", CommunityResult.class));

		List<String> funderList = relation
			.select("target")
			.map((MapFunction<Row, String>) value -> value.getString(0).substring(0, 15), Encoders.STRING())
			.distinct()
			.collectAsList();

		funderList.forEach(funder -> {
			String fundernsp = funder.substring(3);
			String funderdump;
			if (fundernsp.startsWith("corda")) {
				funderdump = "EC_";
				if (fundernsp.endsWith("h2020")) {
					funderdump += "H2020";
				} else {
					funderdump += "FP7";
				}
			} else {
				funderdump = fundernsp.substring(0, fundernsp.indexOf("_")).toUpperCase();
			}
			writeFunderResult(funder, result, outputPath, funderdump);
		});

	}

	private static void dumpResults(String nsp, Dataset<CommunityResult> results, String outputPath,
		String funderName, String funderDump) {

		results.map((MapFunction<CommunityResult, CommunityResult>) r -> {
			if (!Optional.ofNullable(r.getProjects()).isPresent()) {
				return null;
			}
			for (Project p : r.getProjects()) {
				if (p.getId().startsWith(nsp)) {
					if (nsp.startsWith("40|irb")) {
						if (p.getFunder().getShortName().equals(funderName))
							return r;
						else
							return null;
					}
					return r;
				}
			}
			return null;
		}, Encoders.bean(CommunityResult.class))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/" + funderDump);
	}

	private static void writeFunderResult(String funder, Dataset<CommunityResult> results, String outputPath,
		String funderDump) {

		if (funder.startsWith("40|irb")) {
			dumpResults(funder, results, outputPath, "CSF", "HRZZ");
			dumpResults(funder, results, outputPath, "MSES", "MZOS");
		} else
			dumpResults(funder, results, outputPath, funderDump, funderDump);

	}

}
