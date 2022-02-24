
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Node;
import eu.dnetlib.dhp.schema.dump.oaf.graph.RelType;
import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * Create new Relations between Context Entities and Organizations whose products are associated to the context. It
 * produces relation such as: organization <-> isRelatedTo <-> context
 */
public class SparkOrganizationRelation implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkOrganizationRelation.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkOrganizationRelation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_organization_parameters.json"));

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

		final OrganizationMap organizationMap = new Gson()
			.fromJson(parser.get("organizationCommunityMap"), OrganizationMap.class);
		final String serializedOrganizationMap = new Gson().toJson(organizationMap);
		log.info("organization map : {}", serializedOrganizationMap);

		final String communityMapPath = parser.get("communityMapPath");
		log.info("communityMapPath: {}", communityMapPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				extractRelation(spark, inputPath, organizationMap, outputPath, communityMapPath);

			});

	}

	private static void extractRelation(SparkSession spark, String inputPath, OrganizationMap organizationMap,
		String outputPath, String communityMapPath) {

		CommunityMap communityMap = Utils.getCommunityMap(spark, communityMapPath);

		Dataset<Relation> relationDataset = Utils.readPath(spark, inputPath, Relation.class);

		relationDataset.createOrReplaceTempView("relation");

		List<eu.dnetlib.dhp.schema.dump.oaf.graph.Relation> relList = new ArrayList<>();

		Dataset<MergedRels> mergedRelsDataset = spark
			.sql(
				"SELECT target organizationId, source representativeId " +
					"FROM relation " +
					"WHERE datainfo.deletedbyinference = false " +
					"AND relclass = 'merges' " +
					"AND substr(source, 1, 2) = '20'")
			.as(Encoders.bean(MergedRels.class));

		mergedRelsDataset.map((MapFunction<MergedRels, MergedRels>) mergedRels -> {
			if (organizationMap.containsKey(mergedRels.getOrganizationId())) {
				return mergedRels;
			}
			return null;
		}, Encoders.bean(MergedRels.class))
			.filter(Objects::nonNull)
			.collectAsList()
			.forEach(getMergedRelsConsumer(organizationMap, relList, communityMap));

		organizationMap
			.keySet()
			.forEach(
				oId -> organizationMap
					.get(oId)
					.forEach(community -> {
						if (communityMap.containsKey(community)) {
							addRelations(relList, community, oId);
						}
					}));

		spark
			.createDataset(relList, Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	@NotNull
	private static Consumer<MergedRels> getMergedRelsConsumer(OrganizationMap organizationMap,
		List<eu.dnetlib.dhp.schema.dump.oaf.graph.Relation> relList, CommunityMap communityMap) {
		return mergedRels -> {
			String oId = mergedRels.getOrganizationId();
			organizationMap
				.get(oId)
				.forEach(community -> {
					if (communityMap.containsKey(community)) {
						addRelations(relList, community, mergedRels.getRepresentativeId());
					}

				});
			organizationMap.remove(oId);
		};
	}

	private static void addRelations(List<eu.dnetlib.dhp.schema.dump.oaf.graph.Relation> relList, String community,
		String organization) {

		String id = Utils.getContextId(community);
		log.info("create relation for organization: {}", organization);
		relList
			.add(
				eu.dnetlib.dhp.schema.dump.oaf.graph.Relation
					.newInstance(
						Node.newInstance(id, Constants.CONTEXT_ENTITY),
						Node.newInstance(organization, ModelSupport.idPrefixEntity.get(organization.substring(0, 2))),
						RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP),
						Provenance
							.newInstance(
								eu.dnetlib.dhp.oa.graph.dump.Constants.USER_CLAIM,
								eu.dnetlib.dhp.oa.graph.dump.Constants.DEFAULT_TRUST)));

		relList
			.add(
				eu.dnetlib.dhp.schema.dump.oaf.graph.Relation
					.newInstance(
						Node.newInstance(organization, ModelSupport.idPrefixEntity.get(organization.substring(0, 2))),
						Node.newInstance(id, Constants.CONTEXT_ENTITY),
						RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP),
						Provenance
							.newInstance(
								eu.dnetlib.dhp.oa.graph.dump.Constants.USER_CLAIM,
								eu.dnetlib.dhp.oa.graph.dump.Constants.DEFAULT_TRUST)));
	}

}
