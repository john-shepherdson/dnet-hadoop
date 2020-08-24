
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class SparkDumpResultProjectRelation implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkDumpResultProjectRelation.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpResultProjectRelation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_dump_projectrels.json"));

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

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpResultProjectRelations(spark, inputPath, resultPidListPath, outputPath);

			});

	}

	private static void dumpResultProjectRelations(SparkSession spark, String inputPath, String preparedInfoPath,
		String outputPath) {
		Dataset<Relation> relations = Utils.readPath(spark, inputPath + "/relation", Relation.class);
		Dataset<Project> projects = Utils.readPath(spark, inputPath + "/project", Project.class);
		Dataset<ResultPidsList> resultPid = Utils.readPath(spark, preparedInfoPath, ResultPidsList.class);

		relations.createOrReplaceTempView("relation");
		projects.createOrReplaceTempView("project");

		Dataset<ResultProject> resultProj = spark
			.sql(
				"SELECT source resultId ,  code, fundingtree.value fundings" +
					"FROM relation r " +
					"JOIN project p " +
					"ON r.target = p.id " +
					"WHERE r.datainfo.deletedbyinference = false " +
					"AND lower(relclass) = '" + ModelConstants.IS_PRODUCED_BY.toLowerCase() + "'")
			.as(Encoders.bean(ResultProject.class));

		resultProj
			.joinWith(resultPid, resultProj.col("resultId").equalTo(resultPid.col("resultId")), "left")
			.flatMap(
				(FlatMapFunction<Tuple2<ResultProject, ResultPidsList>, eu.dnetlib.dhp.schema.dump.oaf.graph.Relation>) value -> {
					List<eu.dnetlib.dhp.schema.dump.oaf.graph.Relation> relList = new ArrayList<>();
					Optional<ResultPidsList> orel = Optional.ofNullable(value._2());
					if (orel.isPresent()) {
						List<String> projList = new ArrayList<>();
						String code = value._1().getCode();
						for (String fund : value._1().getFundings()) {
							projList.add(Utils.getEntity(fund, code).getId());
						}

						List<KeyValue> resList = orel.get().getResultAllowedPids();
						for (int i = 0; i < resList.size(); i++) {
							String pid = resList.get(i).getKey() + ":" + resList.get(i).getValue();
							for (int j = 0; j < projList.size(); j++) {
								relList
									.addAll(
										Utils
											.getRelationPair(
												pid, projList.get(j), Constants.RESULT, Constants.PROJECT,
												ModelConstants.OUTCOME, ModelConstants.IS_PRODUCED_BY,
												ModelConstants.PRODUCES));

							}
						}

					}

					return relList.iterator();
				}, Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/relationProject");
	}

}
