
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.dump.pidgraph.Entity;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;

public class SparkDumpPidAuthor implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkDumpPidAuthor.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpPidAuthor.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_dump_author.json"));

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

		final List<String> allowedAuthorPids = new Gson().fromJson(parser.get("allowedAuthorPids"), List.class);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpPidAuthor(spark, inputPath, outputPath, allowedAuthorPids);

			});

	}

	private static void dumpPidAuthor(SparkSession spark, String inputPath, String outputPath, List<String> aap) {
		Dataset<Publication> publication = Utils.readPath(spark, inputPath + "/publication", Publication.class);
		Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> dataset = Utils
			.readPath(spark, inputPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class);
		Dataset<Software> software = Utils.readPath(spark, inputPath + "/software", Software.class);
		Dataset<OtherResearchProduct> other = Utils
			.readPath(spark, inputPath + "/otherresearchproduct", OtherResearchProduct.class);

		publication.createOrReplaceTempView("publication");
		dataset.createOrReplaceTempView("dataset");
		software.createOrReplaceTempView("software");
		other.createOrReplaceTempView("other");

		Dataset<KeyValue> pids = spark
			.sql(
				"SELECT DISTINCT apid.value value , apid.qualifier.classid key " +
					"FROM publication " +
					"LATERAL VIEW EXPLODE (author) a as auth " +
					"LATERAL VIEW EXPLODE (auth.pid) p as apid ")
			.as(Encoders.bean(KeyValue.class))
			.union(
				spark
					.sql(
						"SELECT DISTINCT apid.value value , apid.qualifier.classid key " +
							"FROM dataset " +
							"LATERAL VIEW EXPLODE (author) a as auth " +
							"LATERAL VIEW EXPLODE (auth.pid) p as apid ")
					.as(Encoders.bean(KeyValue.class)))
			.union(
				spark
					.sql(
						"SELECT DISTINCT apid.value value , apid.qualifier.classid key " +
							"FROM software " +
							"LATERAL VIEW EXPLODE (author) a as auth " +
							"LATERAL VIEW EXPLODE (auth.pid) p as apid ")
					.as(Encoders.bean(KeyValue.class)))
			.union(
				spark
					.sql(
						"SELECT DISTINCT apid.value value , apid.qualifier.classid key " +
							"FROM other " +
							"LATERAL VIEW EXPLODE (author) a as auth " +
							"LATERAL VIEW EXPLODE (auth.pid) p as apid ")
					.as(Encoders.bean(KeyValue.class)));

		pids.createOrReplaceTempView("pids");

		spark
			.sql(
				"Select distinct key, value " +
					"FROM pids")
			.as(Encoders.bean(KeyValue.class))
			.filter((FilterFunction<KeyValue>) p -> aap.contains(p.getKey()))
			.map(
				(MapFunction<KeyValue, Entity>) pid -> Entity.newInstance(pid.getKey() + ":" + pid.getValue()),
				Encoders.bean(Entity.class))
			.write()

//        resultPids.flatMap((FlatMapFunction<ResultPidsList, Entity>) r-> {
//            List<Entity> ret = new ArrayList<>();
//            r.getAuthorAllowedPids().forEach(pid -> {
//                ret.addAll(pid.stream().map(p -> Entity.newInstance(p.getKey() + ":" + p.getValue())).collect(Collectors.toList()));
//
//            });
//            return ret.iterator();
//        }, Encoders.bean(Entity.class))
//                .write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/author");
	}
}
