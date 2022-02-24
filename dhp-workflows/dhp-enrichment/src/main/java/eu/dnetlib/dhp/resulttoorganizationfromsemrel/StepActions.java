
package eu.dnetlib.dhp.resulttoorganizationfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.readPath;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class StepActions implements Serializable {

	public static void execStep(SparkSession spark,
		String graphPath, String newRelationPath,
		String leavesPath, String chldParentOrgPath, String resultOrgPath) {

		Dataset<Relation> relationGraph = readPath(spark, graphPath, Relation.class);
		// select only the relation source target among those proposed by propagation that are not already existent
		getNewRels(
			newRelationPath, relationGraph,
			getPropagationRelation(spark, leavesPath, chldParentOrgPath, resultOrgPath));

	}

	public static void prepareForNextStep(SparkSession spark, String selectedRelsPath, String resultOrgPath,
		String leavesPath, String chldParentOrgPath, String leavesOutputPath,
		String orgOutputPath) {
		// use of the parents as new leaves set
		changeLeavesSet(spark, leavesPath, chldParentOrgPath, leavesOutputPath);

		// add the new relations obtained from propagation to the keyvalueset result organization
		updateResultOrganization(
			spark, resultOrgPath, readPath(spark, selectedRelsPath, Relation.class), orgOutputPath);
	}

	private static void updateResultOrganization(SparkSession spark, String resultOrgPath,
		Dataset<Relation> selectedRels, String outputPath) {
		Dataset<KeyValueSet> resultOrg = readPath(spark, resultOrgPath, KeyValueSet.class);
		resultOrg
			.joinWith(
				selectedRels, resultOrg
					.col("key")
					.equalTo(selectedRels.col("source")),
				"left")
			.groupByKey((MapFunction<Tuple2<KeyValueSet, Relation>, String>) mf -> mf._1().getKey(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Tuple2<KeyValueSet, Relation>, KeyValueSet>) (key, it) -> {
				Tuple2<KeyValueSet, Relation> first = it.next();
				if (!Optional.ofNullable(first._2()).isPresent()) {
					return first._1();
				}
				KeyValueSet ret = new KeyValueSet();
				ret.setKey(first._1().getKey());
				HashSet<String> hs = new HashSet<>();
				hs.addAll(first._1().getValueSet());
				hs.add(first._2().getTarget());
				it.forEachRemaining(rel -> hs.add(rel._2().getTarget()));
				ArrayList<String> orgs = new ArrayList<>();
				orgs.addAll(hs);
				ret.setValueSet(orgs);
				return ret;
			}, Encoders.bean(KeyValueSet.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static void changeLeavesSet(SparkSession spark, String leavesPath, String chldParentOrgPath,
		String leavesOutputPath) {
		Dataset<KeyValueSet> childParent = readPath(spark, chldParentOrgPath, KeyValueSet.class);
		Dataset<Leaves> leaves = readPath(spark, leavesPath, Leaves.class);

		childParent.createOrReplaceTempView("childParent");
		leaves.createOrReplaceTempView("leaves");

		spark
			.sql(
				"SELECT distinct parent as value " +
					"FROM leaves " +
					"JOIN (SELECT key, parent " +
					"      FROM childParent " +
					"      LATERAL VIEW explode(valueSet) kv as parent) tmp " +
					"ON value = key ")
			.as(Encoders.bean(Leaves.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(leavesOutputPath);
	}

	@NotNull
	private static void getNewRels(String newRelationPath, Dataset<Relation> relationDataset,
		Dataset<Relation> newRels) {
		// selects new, not already existent relations
		// union of new propagation relations to the relation set
		// grouping from sourcetarget (we are sure the only relations are those from result to organization by
		// construction of the set)
		// if at least one relation in the set was not produced by propagation no new relation will be returned

		relationDataset
			.union(newRels)
			.groupByKey((MapFunction<Relation, String>) r -> r.getSource() + r.getTarget(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Relation, String>) (k, it) -> {

				ArrayList<Relation> relationList = new ArrayList<>();
				relationList.add(it.next());
				it.forEachRemaining(rel -> relationList.add(rel));

				if (relationList
					.stream()
					.filter(
						rel -> !rel
							.getDataInfo()
							.getProvenanceaction()
							.getClassid()
							.equals(PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID))
					.count() > 0) {
					return null;
				}

				return new ObjectMapper().writeValueAsString(relationList.get(0));

			}, Encoders.STRING())
			.filter(Objects::nonNull)
			.map(
				(MapFunction<String, Relation>) r -> new ObjectMapper().readValue(r, Relation.class),
				Encoders.bean(Relation.class))
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(newRelationPath);

	}

	// get the possible relations from propagation
	private static Dataset<Relation> getPropagationRelation(SparkSession spark,
		String leavesPath,
		String chldParentOrgPath,
		String resultOrgPath) {

		Dataset<KeyValueSet> childParent = readPath(spark, chldParentOrgPath, KeyValueSet.class);
		Dataset<KeyValueSet> resultOrg = readPath(spark, resultOrgPath, KeyValueSet.class);
		Dataset<Leaves> leaves = readPath(spark, leavesPath, Leaves.class);

		childParent.createOrReplaceTempView("childParent");
		resultOrg.createOrReplaceTempView("resultOrg");
		leaves.createOrReplaceTempView("leaves");

		Dataset<KeyValueSet> resultParent = spark
			.sql(
				"SELECT  resId as key, " +
					"collect_set(parent) valueSet " +
					"FROM (SELECT key as child, parent " +
					"      FROM childParent  " +
					"      LATERAL VIEW explode(valueSet) ks as parent) as cp " +
					"JOIN leaves " +
					"ON leaves.value = cp.child " +
					"JOIN (" +
					"SELECT key as resId, org " +
					"FROM resultOrg " +
					"LATERAL VIEW explode (valueSet) ks as org ) as ro " +
					"ON  leaves.value = ro.org " +
					"GROUP BY resId")
			.as(Encoders.bean(KeyValueSet.class));

		// create new relations from result to organization for each result linked to a leaf
		return resultParent
			.flatMap(
				(FlatMapFunction<KeyValueSet, Relation>) v -> v
					.getValueSet()
					.stream()
					.map(
						orgId -> getRelation(
							v.getKey(),
							orgId,
							ModelConstants.HAS_AUTHOR_INSTITUTION,
							ModelConstants.RESULT_ORGANIZATION,
							ModelConstants.AFFILIATION,
							PROPAGATION_DATA_INFO_TYPE,
							PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID,
							PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME))
					.collect(Collectors.toList())
					.iterator(),
				Encoders.bean(Relation.class));

	}

}
