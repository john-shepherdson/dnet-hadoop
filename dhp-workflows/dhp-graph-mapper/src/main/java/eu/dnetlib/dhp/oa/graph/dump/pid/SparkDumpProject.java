
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.io.StringReader;
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
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.pidgraph.Entity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;

public class SparkDumpProject implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkDumpOrganization.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpOrganization.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_dump_project.json"));

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
				dumpProjects(spark, inputPath, outputPath);

			});
	}

	private static void dumpProjects(SparkSession spark, String inputPath, String outputPath) {
		Dataset<Project> projectDataset = Utils.readPath(spark, inputPath, Project.class);

		projectDataset.flatMap((FlatMapFunction<Project, Entity>) project -> {
			List<Entity> projs = new ArrayList<>();
			project.getFundingtree().forEach(fund -> {
				try {
					projs.add(Utils.getEntity(fund.getValue(), project.getCode().getValue()));
				} catch (DocumentException e) {
					e.printStackTrace();
				}
			});
			return projs.iterator();
		}, Encoders.bean(Entity.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/project");

	}
}
