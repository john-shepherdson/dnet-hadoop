
package eu.dnetlib.dhp.oa.graph.dump.pid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.ResultProject;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import eu.dnetlib.dhp.schema.oaf.Project;

public class SparkDumpResultRelation implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkDumpResultRelation.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpResultRelation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_pid/input_dump_result.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("preparedInfoPath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpPidRelations(spark, inputPath, outputPath);

			});

	}

	private static Dataset<Relation> distinctRelations(Dataset<Relation> rels) {
		return rels
			.filter(getRelationFilterFunction())
			.groupByKey(
				(MapFunction<Relation, String>) r -> String
					.join(
						r.getSource().getId(), r.getTarget().getId(), r.getReltype().getName(),
						r.getReltype().getType()),
				Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Relation, Relation>) (key, relationIterator) -> relationIterator.next(),
				Encoders.bean(Relation.class));
	}

	private static FilterFunction<Relation> getRelationFilterFunction() {
		return (FilterFunction<Relation>) r -> StringUtils.isNotBlank(r.getSource().getId()) ||
			StringUtils.isNotBlank(r.getTarget().getId()) ||
			StringUtils.isNotBlank(r.getReltype().getName()) ||
			StringUtils.isNotBlank(r.getReltype().getType());
	}

	private static void dumpPidRelations(SparkSession spark, String inputPath, String outputPath) {
		Dataset<ResultPidsList> resultPids = Utils.readPath(spark, inputPath, ResultPidsList.class);

		distinctRelations(resultPids.flatMap((FlatMapFunction<ResultPidsList, Relation>) r -> {
			List<Relation> ret = new ArrayList<>();
			List<KeyValue> resPids = r.getResultAllowedPids();
			List<List<KeyValue>> authPids = r.getAuthorAllowedPids();

			for (int i = 0; i < resPids.size() - 1; i++) {
				String pid = resPids.get(i).getKey() + ":" + resPids.get(i).getValue();
				for (int j = i + 1; j < resPids.size(); j++) {
					ret
						.addAll(
							Utils
								.getRelationPair(
									pid, resPids.get(j).getKey() + ":" + resPids.get(j).getValue(),
									Constants.RESULT, Constants.RESULT, Constants.SIMILARITY,
									Constants.RESPID_RESPID_RELATION, Constants.RESPID_RESPID_RELATION));
				}
			}

			for (int i = 0; i < authPids.size() - 1; i++) {
				for (int j = i + 1; j < authPids.size(); j++) {
					ret.addAll(getAuthRelations(authPids.get(i), authPids.get(j)));
				}
			}

			for (int i = 0; i < resPids.size(); i++) {
				String pid = resPids.get(i).getKey() + ":" + resPids.get(i).getValue();
				for (int j = 0; j < authPids.size(); j++) {
					for (int k = 0; k < authPids.get(j).size(); k++) {
						ret
							.addAll(
								Utils
									.getRelationPair(
										pid,
										authPids.get(j).get(k).getKey() + ":" + authPids.get(j).get(k).getValue(),
										Constants.RESULT, Constants.AUTHOR, Constants.AUTHORSHIP,
										Constants.RES_AUTHOR_RELATION, Constants.AUTHOR_RES_RELATION));
					}

				}
			}
			return ret.iterator();
		}, Encoders.bean(Relation.class)))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/relation");
	}

	private static List<Relation> getAuthRelations(List<KeyValue> a1, List<KeyValue> a2) {
		List<Relation> ret = new ArrayList<>();
		if (a1.size() > 1) {
			ret.addAll(getSameAs(a1));
		}
		if (a2.size() > 1) {
			ret.addAll(getSameAs(a2));
		}
		for (int i = 0; i < a1.size(); i++) {
			String pid = a1.get(i).getKey() + ":" + a1.get(i).getValue();
			for (int j = 0; j < a2.size(); j++) {
				ret
					.addAll(
						Utils
							.getRelationPair(
								pid, a2.get(j).getKey() + ":" + a2.get(j).getValue(),
								Constants.AUTHOR, Constants.AUTHOR, Constants.AUTHORSHIP,
								Constants.AUTHOR_AUTHOR_RELATION, Constants.AUTHOR_AUTHOR_RELATION));
			}
		}

		return ret;
	}

	private static List<Relation> getSameAs(List<KeyValue> a1) {
		List<Relation> ret = new ArrayList<>();
		for (int i = 0; i < a1.size() - 1; i++) {
			ret
				.addAll(
					Utils
						.getRelationPair(
							a1.get(i).getKey() + ":" + a1.get(i).getValue(),
							a1.get(i + 1).getKey() + ":" + a1.get(i + 1).getValue(),
							Constants.AUTHOR, Constants.AUTHOR, Constants.SIMILARITY,
							Constants.SAME_AS, Constants.SAME_AS));
		}
		return ret;
	}
}
