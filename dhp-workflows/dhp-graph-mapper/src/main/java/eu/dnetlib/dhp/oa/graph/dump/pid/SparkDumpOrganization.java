
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.pidgraph.Entity;
import eu.dnetlib.dhp.schema.oaf.Organization;

public class SparkDumpOrganization implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkDumpOrganization.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpOrganization.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_dump_organization.json"));

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

		final List<String> allowedOrgPid = new Gson().fromJson(parser.get("allowedOrganizationPids"), List.class);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpPidOrgs(spark, allowedOrgPid, inputPath, outputPath);

			});

	}

	private static void dumpPidOrgs(SparkSession spark, List<String> allowedOrgPid, String inputPath,
		String outputPath) {
		Dataset<Organization> resultPids = Utils.readPath(spark, inputPath, Organization.class);

		resultPids.flatMap((FlatMapFunction<Organization, Entity>) r -> {
			List<Entity> ret = new ArrayList<>();
			r.getPid().forEach(pid -> {
				if (allowedOrgPid.contains(pid.getQualifier().getClassid().toLowerCase())) {
					ret.add(Entity.newInstance(pid.getQualifier().getClassid() + ":" + pid.getValue()));
				}
			});
			return ret.iterator();
		}, Encoders.bean(Entity.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/organization");
	}
}
