
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
import eu.dnetlib.dhp.schema.scholexplorer.DLIRelation;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class SparkPropagateRelationsJob {

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

		final String relationPath = parser.get("relationPath");
		final String mergeRelPath = parser.get("mergeRelPath");
		final String targetRelPath = parser.get("targetRelPath");

		final Dataset<Relation> merge = spark
			.read()
			.load(mergeRelPath)
			.as(Encoders.bean(Relation.class))
			.where("relClass == 'merges'");

		final Dataset<DLIRelation> rels = spark
			.read()
			.load(relationPath)
			.as(Encoders.kryo(DLIRelation.class))
			.map(
				(MapFunction<DLIRelation, DLIRelation>) r -> r,
				Encoders.bean(DLIRelation.class));

		final Dataset<DLIRelation> firstJoin = rels
			.joinWith(merge, merge.col("target").equalTo(rels.col("source")), "left_outer")
			.map(
				(MapFunction<Tuple2<DLIRelation, Relation>, DLIRelation>) r -> {
					final Relation mergeRelation = r._2();
					final DLIRelation relation = r._1();
					if (mergeRelation != null)
						relation.setSource(mergeRelation.getSource());
					return relation;
				},
				Encoders.bean(DLIRelation.class));

		final Dataset<DLIRelation> secondJoin = firstJoin
			.joinWith(merge, merge.col("target").equalTo(firstJoin.col("target")), "left_outer")
			.map(
				(MapFunction<Tuple2<DLIRelation, Relation>, DLIRelation>) r -> {
					final Relation mergeRelation = r._2();
					final DLIRelation relation = r._1();
					if (mergeRelation != null)
						relation.setTarget(mergeRelation.getSource());
					return relation;
				},
				Encoders.kryo(DLIRelation.class));

		secondJoin.write().mode(SaveMode.Overwrite).save(targetRelPath);
	}
}
