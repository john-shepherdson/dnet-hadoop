
package eu.dnetlib.dhp.oa.dedup;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.kwartile.lib.cc.ConnectedComponent;

import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;

public class OpenorgsUtility {

	public static final String GROUP_PREFIX = "group::";

	public static long hash(final String id) {
		return Hashing.murmur3_128().hashString(id).asLong();
	}

	// create families (group of connected components using specified relation): <id, familyId>
	public static Dataset<Row> createFamilies(SparkSession spark, String relationPath, String relClass) {
		Dataset<Row> parentChildRels = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(relationPath)
			.where(col("relClass").equalTo(relClass))
			.select("source", "target");

		UserDefinedFunction hashUDF = functions
			.udf(
				(String s) -> hash(s), DataTypes.LongType);

		// <hash(id), id>
		Dataset<Row> vertexIdMap = parentChildRels
			.selectExpr("source as id")
			.union(parentChildRels.selectExpr("target as id"))
			.distinct()
			.withColumn("vertexId", hashUDF.apply(functions.col("id")));

		// transform simrels into pairs of numeric ids
		final Dataset<Row> edges = parentChildRels
			.withColumn("source", hashUDF.apply(functions.col("source")))
			.withColumn("target", hashUDF.apply(functions.col("target")));

		// resolve connected components
		// ("vertexId", "familyId")
		Dataset<Row> cliques = ConnectedComponent
			.runOnPairs(edges, 50, spark);

		// transform "vertexId" back to its original string value
		// groupId is kept numeric as its string value is not used
		// ("id", "familyId")
		return cliques
			.join(vertexIdMap, JavaConversions.asScalaBuffer(Collections.singletonList("vertexId")), "inner")
			.drop("vertexId")
			.distinct();
	}

	public static boolean filterRels(Relation rel, String relClass, String relType, String subRelType) {
		return rel.getRelClass().equals(relClass)
			&& rel.getRelType().equals(relType)
			&& rel.getSubRelType().equals(subRelType);
	}

	public static JavaRDD<Tuple2<Tuple2<String, String>, String>> collectRels(SparkSession spark, String relationPath,
		String relClass, String relType, String subRelType, boolean bestAtSource) {

		JavaRDD<Relation> filteredRels = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(relationPath)
			.as(Encoders.bean(Relation.class))
			.map(AbstractSparkAction.patchRelFn(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(r -> filterRels(r, relClass, relType, subRelType));

		return filteredRels
			.map(
				rel -> {
					// put the best id as source (if bestAtSource flag is enabled): <best id, other id>
					if (!bestAtSource || DedupUtility.compareOpenOrgIds(rel.getSource(), rel.getTarget()) < 0)
						return new Tuple2<>(new Tuple2<>(rel.getSource(), rel.getTarget()), relClass);
					else
						return new Tuple2<>(new Tuple2<>(rel.getTarget(), rel.getSource()), relClass);
				})
			.distinct();
	}

	public static JavaRDD<Tuple3<String, String, String>> processMergeRels(SparkSession spark, String mergeRelsPath,
		JavaRDD<Tuple2<Tuple2<String, String>, String>> diffRels,
		JavaRDD<Tuple2<Tuple2<String, String>, String>> parentChildRels) {
		// create duplicate suggestions relations: <<source, target>, group id>>
		JavaRDD<Tuple2<Tuple2<String, String>, String>> rawOpenorgsRels = spark
			.read()
			.load(mergeRelsPath)
			.as(Encoders.bean(Relation.class))
			.where("relClass == 'merges'")
			.toJavaRDD()
			.mapToPair(r -> new Tuple2<>(r.getSource(), r.getTarget()))
			.filter(t -> !t._2().contains("openorgsmesh")) // remove openorgsmesh: they are only for dedup
			.groupByKey()
			.map(g -> Lists.newArrayList(g._2()))
			.filter(l -> l.size() > 1)
			.flatMap(l -> {
				String groupId = GROUP_PREFIX + UUID.randomUUID();
				List<String> ids = sortIds(l); // sort IDs by type
				List<Tuple2<Tuple2<String, String>, String>> rels = new ArrayList<>();
				String source = ids.get(0);
				for (String target : ids) {
					rels.add(new Tuple2<>(new Tuple2<>(source, target), groupId));
				}

				return rels.iterator();
			});

		// filter out DiffRels and ParentChildRels
		JavaRDD<Tuple3<String, String, String>> openorgsRels = rawOpenorgsRels
			.union(diffRels)
			.union(parentChildRels)
			// concatenation of source and target: <source|||target, group id> or <source|||target, "diffRel"> or
			// <source|||target, "parentChildRel">
			.mapToPair(t -> new Tuple2<>(t._1()._1() + "@@@" + t._1()._2(), t._2()))
			.groupByKey()
			.map(
				g -> new Tuple2<>(g._1(), StreamSupport
					.stream(g._2().spliterator(), false)
					.collect(Collectors.toList())))
			// <source|||target, list(group_id, "diffRel")>: take only relations with only the group_id, it
			// means they are correct. If the diffRel is present the relation has to be removed
			.filter(g -> g._2().size() == 1 && g._2().get(0).contains(GROUP_PREFIX))
			.map(
				t -> new Tuple3<>(
					t._1().split("@@@")[0],
					t._1().split("@@@")[1],
					t._2().get(0)));
		return openorgsRels;
	}

	// Sort IDs based on the type. Priority: 1) openorgs, 2)corda, 3)alphabetic
	public static List<String> sortIds(List<String> ids) {
		ids.sort(DedupUtility::compareOpenOrgIds);
		return ids;
	}
}
