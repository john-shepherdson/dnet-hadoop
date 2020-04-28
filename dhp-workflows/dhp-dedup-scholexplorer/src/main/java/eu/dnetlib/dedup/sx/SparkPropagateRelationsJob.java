
package eu.dnetlib.dedup.sx;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class SparkPropagateRelationsJob {
	enum FieldType {
		SOURCE, TARGET
	}

	static final String SOURCEJSONPATH = "$.source";
	static final String TARGETJSONPATH = "$.target";

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkPropagateRelationsJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/sx/dedup/dedup_propagate_relation_parameters.json")));
		parser.parseArgument(args);
		final SparkSession spark = SparkSession
			.builder()
			.appName(SparkUpdateEntityJob.class.getSimpleName())
			.master(parser.get("master"))
			.getOrCreate();

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		final String relationPath = parser.get("relationPath");
		final String mergeRelPath = parser.get("mergeRelPath");
		final String targetRelPath = parser.get("targetRelPath");

		final Dataset<Relation> merge = spark
			.read()
			.load(mergeRelPath)
			.as(Encoders.bean(Relation.class))
			.where("relClass == 'merges'");

		final Dataset<Relation> rels = spark.read().load(relationPath).as(Encoders.bean(Relation.class));

		final Dataset<Relation> firstJoin = rels
			.joinWith(merge, merge.col("target").equalTo(rels.col("source")), "left_outer")
			.map(
				(MapFunction<Tuple2<Relation, Relation>, Relation>) r -> {
					final Relation mergeRelation = r._2();
					final Relation relation = r._1();

					if (mergeRelation != null)
						relation.setSource(mergeRelation.getSource());
					return relation;
				},
				Encoders.bean(Relation.class));

		final Dataset<Relation> secondJoin = firstJoin
			.joinWith(merge, merge.col("target").equalTo(firstJoin.col("target")), "left_outer")
			.map(
				(MapFunction<Tuple2<Relation, Relation>, Relation>) r -> {
					final Relation mergeRelation = r._2();
					final Relation relation = r._1();
					if (mergeRelation != null)
						relation.setTarget(mergeRelation.getSource());
					return relation;
				},
				Encoders.bean(Relation.class));

		secondJoin.write().mode(SaveMode.Overwrite).save(targetRelPath);
	}

	private static boolean containsDedup(final String json) {
		final String source = DHPUtils.getJPathString(SOURCEJSONPATH, json);
		final String target = DHPUtils.getJPathString(TARGETJSONPATH, json);

		return source.toLowerCase().contains("dedup") || target.toLowerCase().contains("dedup");
	}

	private static String replaceField(final String json, final String id, final FieldType type) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			Relation relation = mapper.readValue(json, Relation.class);
			if (relation.getDataInfo() == null)
				relation.setDataInfo(new DataInfo());
			relation.getDataInfo().setDeletedbyinference(false);
			switch (type) {
				case SOURCE:
					relation.setSource(id);
					return mapper.writeValueAsString(relation);
				case TARGET:
					relation.setTarget(id);
					return mapper.writeValueAsString(relation);
				default:
					throw new IllegalArgumentException("");
			}
		} catch (IOException e) {
			throw new RuntimeException("unable to deserialize json relation: " + json, e);
		}
	}
}
