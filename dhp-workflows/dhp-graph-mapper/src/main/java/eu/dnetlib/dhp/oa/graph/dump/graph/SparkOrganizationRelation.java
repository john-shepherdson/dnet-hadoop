
package eu.dnetlib.dhp.oa.graph.dump.graph;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Node;
import eu.dnetlib.dhp.schema.dump.oaf.graph.RelType;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SparkOrganizationRelation implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkOrganizationRelation.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkOrganizationRelation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_whole/input_organization_parameters.json"));

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
		log.info("organization map : {}", new Gson().toJson(organizationMap));

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				extractRelation(spark, inputPath, organizationMap, outputPath);

			});

	}

	private static void extractRelation(SparkSession spark, String inputPath, OrganizationMap organizationMap,
		String outputPath) {
		Dataset<Relation> relationDataset = Utils.readPath(spark, inputPath, Relation.class);

		relationDataset.createOrReplaceTempView("relation");
		Set<String> organizationSet = organizationMap.keySet();

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
			.forEach(mergedRels -> {
				String oId = mergedRels.getOrganizationId();
				organizationSet.remove(oId);
				organizationMap
					.get(oId)
					.forEach(community -> addRelations(relList, community, mergedRels.getRepresentativeId()));
			});

		organizationSet
			.forEach(
				oId -> organizationMap
					.get(oId)
					.forEach(community -> addRelations(relList, community, oId)));

		spark
			.createDataset(relList, Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

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
