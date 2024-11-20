
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.api.model.CommunityEntityMap;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class PrepareResultCommunitySet {

	private static final Logger log = LoggerFactory.getLogger(PrepareResultCommunitySet.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultCommunitySet.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/resulttocommunityfromorganization/input_preparecommunitytoresult_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String baseURL = parser.get("baseURL");
		log.info("baseURL: {}", baseURL);

		final CommunityEntityMap organizationMap = Utils.getCommunityOrganization(baseURL);
		//final CommunityEntityMap organizationMap = Utils.getOrganizationCommunityMap(baseURL);
		log.info("organizationMap: {}", new Gson().toJson(organizationMap));

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareInfo(spark, inputPath, outputPath, organizationMap);
			});
	}

	private static void prepareInfo(
		SparkSession spark,
		String inputPath,
		String outputPath,
		CommunityEntityMap organizationMap) {

		Dataset<Relation> relation = readPath(spark, inputPath, Relation.class);
		relation.createOrReplaceTempView("relation");

		String query = "SELECT result_organization.source resultId, result_organization.target orgId, org_set merges "
			+ "FROM (SELECT source, target "
			+ "      FROM relation "
			+ "      WHERE datainfo.deletedbyinference = false "
			+ "      AND lower(relClass) = '"
			+ ModelConstants.HAS_AUTHOR_INSTITUTION.toLowerCase()
			+ "') result_organization "
			+ "LEFT JOIN (SELECT source, collect_set(target) org_set "
			+ "      FROM relation "
			+ "      WHERE datainfo.deletedbyinference = false "
			+ "      AND lower(relClass) = '"
			+ ModelConstants.MERGES.toLowerCase()
			+ "' "
			+ "      GROUP BY source) organization_organization "
			+ "ON result_organization.target = organization_organization.source ";

		Dataset<ResultOrganizations> result_organizationset = spark
			.sql(query)
			.as(Encoders.bean(ResultOrganizations.class));

		result_organizationset
			.map(mapResultCommunityFn(organizationMap), Encoders.bean(ResultCommunityList.class))
			.filter(Objects::nonNull)
			.toJavaRDD()
			.mapToPair(value -> new Tuple2<>(value.getResultId(), value))
			.reduceByKey((a, b) -> {
				ArrayList<String> cl = a.getCommunityList();
				b.getCommunityList().stream().forEach(s -> {
					if (!cl.contains(s)) {
						cl.add(s);
					}
				});
				a.setCommunityList(cl);
				return a;
			})
			.map(value -> OBJECT_MAPPER.writeValueAsString(value._2()))
			.saveAsTextFile(outputPath, GzipCodec.class);
	}

	private static MapFunction<ResultOrganizations, ResultCommunityList> mapResultCommunityFn(
		CommunityEntityMap organizationMap) {
		return value -> {
			String rId = value.getResultId();
			Optional<List<String>> orgs = Optional.ofNullable(value.getMerges());
			String oTarget = value.getOrgId();
			Set<String> communitySet = new HashSet<>();
			if (organizationMap.containsKey(oTarget)) {
				communitySet.addAll(organizationMap.get(oTarget));
			}
			if (orgs.isPresent())
				for (String oId : orgs.get()) {
					if (organizationMap.containsKey(oId)) {
						communitySet.addAll(organizationMap.get(oId));
					}
				}
			if (!communitySet.isEmpty()) {
				ResultCommunityList rcl = new ResultCommunityList();
				rcl.setResultId(rId);
				ArrayList<String> communityList = new ArrayList<>();
				communityList.addAll(communitySet);
				rcl.setCommunityList(communityList);
				return rcl;
			}
			return null;
		};
	}
}
