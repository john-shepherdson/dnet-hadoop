
package eu.dnetlib.dedup.sx;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.scholexplorer.OafUtils;
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

		final Dataset<Relation> rels = spark
			.read()
			.load(relationPath)
			.as(Encoders.kryo(Relation.class))
			.map(
				(MapFunction<Relation, Relation>) r -> r,
				Encoders.bean(Relation.class));

		final Dataset<Relation> firstJoin = rels
			.joinWith(merge, merge.col("target").equalTo(rels.col("source")), "left_outer")
			.map(
				(MapFunction<Tuple2<Relation, Relation>, Relation>) r -> {
					final Relation mergeRelation = r._2();
					final Relation relation = r._1();
					if (mergeRelation != null)
						relation.setSource(mergeRelation.getSource());
					if (relation.getDataInfo() == null)
						relation.setDataInfo(OafUtils.generateDataInfo("0.9", false));
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
				Encoders.kryo(Relation.class));

		secondJoin.write().mode(SaveMode.Overwrite).save(targetRelPath);
	}
}
