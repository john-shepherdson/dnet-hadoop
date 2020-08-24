
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class SparkDumpResultOrganizationRelation implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkDumpResultOrganizationRelation.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpResultOrganizationRelation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_dump_organizationrels.json"));

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

		final String resultPidListPath = parser.get("preparedInfoPath");

		final List<String> allowedPids = new Gson().fromJson(parser.get("allowedOrganizationPids"), List.class);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpResultOrganziationRelations(spark, inputPath, resultPidListPath, allowedPids, outputPath);

			});

	}

	private static void dumpResultOrganziationRelations(SparkSession spark, String inputPath, String preparedInfoPath,
		List<String> allowedPids, String outputPath) {
		Dataset<Relation> relations = Utils.readPath(spark, inputPath + "/relation", Relation.class);
		Dataset<Organization> organizations = Utils.readPath(spark, inputPath + "/organization", Organization.class);
		Dataset<ResultPidsList> resultPid = Utils.readPath(spark, preparedInfoPath, ResultPidsList.class);

		relations.createOrReplaceTempView("relation");
		organizations.createOrReplaceTempView("organization");

		Dataset<ResultOrganization> resultOrg = spark
			.sql(
				"SELECT source resultId ,  pid orgPids" +
					"FROM relation r " +
					"JOIN organization o " +
					"ON r.target = o.id " +
					"WHERE r.datainfo.deletedbyinference = false " +
					"AND o.datainfo.deletedbyinference = false " +
					"AND lower(relclass) = '" + ModelConstants.HAS_AUTHOR_INSTITUTION.toLowerCase() + "'")
			.as(Encoders.bean(ResultOrganization.class));

		resultOrg
			.joinWith(resultPid, resultOrg.col("resultId").equalTo(resultPid.col("resultId")), "left")
			.flatMap(
				(FlatMapFunction<Tuple2<ResultOrganization, ResultPidsList>, eu.dnetlib.dhp.schema.dump.oaf.graph.Relation>) value -> {
					List<eu.dnetlib.dhp.schema.dump.oaf.graph.Relation> relList = new ArrayList<>();
					Optional<ResultPidsList> orel = Optional.ofNullable(value._2());
					if (orel.isPresent()) {
						List<String> orgList = value
							._1()
							.getOrgPid()
							.stream()
							.filter(p -> allowedPids.contains(p.getQualifier().getClassid()))
							.map(pid -> pid.getQualifier().getClassid() + ":" + pid.getValue())
							.collect(Collectors.toList());
						if (orgList.size() > 0) {
							List<KeyValue> resList = orel.get().getResultAllowedPids();
							for (int i = 0; i < resList.size(); i++) {
								String pid = resList.get(i).getKey() + ":" + resList.get(i).getValue();
								for (int j = 0; j < orgList.size(); j++) {
									relList
										.addAll(
											Utils
												.getRelationPair(
													pid, orgList.get(j), Constants.RESULT, Constants.ORGANIZATION,
													ModelConstants.AFFILIATION, ModelConstants.HAS_AUTHOR_INSTITUTION,
													ModelConstants.IS_AUTHOR_INSTITUTION_OF));

								}
							}
						}
					}

					return relList.iterator();
				}, Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/relationOrganization");
	}
}
