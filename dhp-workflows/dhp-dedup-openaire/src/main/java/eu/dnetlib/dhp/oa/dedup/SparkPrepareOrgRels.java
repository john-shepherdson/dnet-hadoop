
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.model.OrgSimRel;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;
import scala.Tuple3;

public class SparkPrepareOrgRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkPrepareOrgRels.class);
	public static final String GROUP_PREFIX = "group::";

	public SparkPrepareOrgRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkPrepareOrgRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/prepareOrgRels_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkPrepareOrgRels(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService) throws IOException {

		final String graphBasePath = parser.get("graphBasePath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");
		final int numConnections = Optional
			.ofNullable(parser.get("numConnections"))
			.map(Integer::valueOf)
			.orElse(NUM_CONNECTIONS);

		final String dbUrl = parser.get("dbUrl");
		final String dbTable = parser.get("dbTable");
		final String dbUser = parser.get("dbUser");
		final String dbPwd = parser.get("dbPwd");

		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);
		log.info("numPartitions: '{}'", numConnections);
		log.info("dbUrl:         '{}'", dbUrl);
		log.info("dbUser:        '{}'", dbUser);
		log.info("table:         '{}'", dbTable);
		log.info("dbPwd:         '{}'", "xxx");

		final String organization = ModelSupport.getMainType(EntityType.organization);
		final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, organization);
		final String entityPath = DedupUtility.createEntityPath(graphBasePath, organization);
		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		Dataset<OrgSimRel> relations = createRelations(spark, mergeRelPath, relationPath, entityPath);

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);

		relations
			.repartition(numConnections)
			.write()
			.mode(SaveMode.Overwrite)
			.jdbc(dbUrl, dbTable, connectionProperties);

	}

	private static boolean filterRels(Relation rel, String entityType) {

		switch (entityType) {
			case "result":
				if (rel.getRelClass().equals(ModelConstants.IS_DIFFERENT_FROM)
					&& rel.getRelType().equals(ModelConstants.RESULT_RESULT)
					&& rel.getSubRelType().equals(ModelConstants.DEDUP))
					return true;
				break;
			case "organization":
				if (rel.getRelClass().equals(ModelConstants.IS_DIFFERENT_FROM)
					&& rel.getRelType().equals(ModelConstants.ORG_ORG_RELTYPE)
					&& rel.getSubRelType().equals(ModelConstants.DEDUP))
					return true;
				break;
			default:
				return false;
		}
		return false;
	}

	// create openorgs simrels <best id, other id> starting from mergerels, remove the diffrels
	public static Dataset<OrgSimRel> createRelations(
		final SparkSession spark,
		final String mergeRelsPath,
		final String relationPath,
		final String entitiesPath) {

		// collect diffrels from the raw graph relations: <<best id, other id>, "diffRel">
		JavaRDD<Tuple2<Tuple2<String, String>, String>> diffRels = spark
			.read()
			.textFile(relationPath)
			.map(patchRelFn(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(r -> filterRels(r, "organization"))
			// put the best id as source of the diffrel: <best id, other id>
			.map(rel -> {
				if (DedupUtility.compareOpenOrgIds(rel.getSource(), rel.getTarget()) < 0)
					return new Tuple2<>(new Tuple2<>(rel.getSource(), rel.getTarget()), "diffRel");
				else
					return new Tuple2<>(new Tuple2<>(rel.getTarget(), rel.getSource()), "diffRel");
			})
			.distinct();
		log.info("Number of DiffRels collected: {}", diffRels.count());

		// collect all the organizations
		Dataset<Tuple2<String, Organization>> entities = spark
			.read()
			.textFile(entitiesPath)
			.map(
				(MapFunction<String, Tuple2<String, Organization>>) it -> {
					Organization entity = OBJECT_MAPPER.readValue(it, Organization.class);
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Organization.class)));

		// relations with their group (connected component id)
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
		log.info("Number of Raw Openorgs Relations created: {}", rawOpenorgsRels.count());

		// filter out diffRels
		JavaRDD<Tuple3<String, String, String>> openorgsRels = rawOpenorgsRels
			.union(diffRels)
			// concatenation of source and target: <source|||target, group id> or <source|||target, "diffRel">
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
		log.info("Number of Openorgs Relations created: '{}'", openorgsRels.count());

		// <best ID basing on priority, ID, groupID>
		Dataset<Tuple3<String, String, String>> relations = spark
			.createDataset(
				openorgsRels.rdd(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

		// create orgsimrels
		Dataset<Tuple2<String, OrgSimRel>> relations2 = relations
			.joinWith(entities, relations.col("_2").equalTo(entities.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple3<String, String, String>, Tuple2<String, Organization>>, OrgSimRel>) r -> {
					final Organization o = r._2()._2();
					return new OrgSimRel(
						r._1()._1(),
						Optional.ofNullable(o.getOriginalId()).map(oid -> oid.get(0)).orElse(null),
						Optional.ofNullable(o.getLegalname()).map(Field::getValue).orElse(""),
						Optional.ofNullable(o.getLegalshortname()).map(Field::getValue).orElse(""),
						Optional.ofNullable(o.getCountry()).map(Qualifier::getClassid).orElse(""),
						Optional.ofNullable(o.getWebsiteurl()).map(Field::getValue).orElse(""),
						Optional
							.ofNullable(o.getCollectedfrom())
							.map(c -> Optional.ofNullable(c.get(0)).map(KeyValue::getValue).orElse(""))
							.orElse(""),
						r._1()._3(),
						structuredPropertyListToString(o.getPid()),
						parseECField(o.getEclegalbody()),
						parseECField(o.getEclegalperson()),
						parseECField(o.getEcnonprofit()),
						parseECField(o.getEcresearchorganization()),
						parseECField(o.getEchighereducation()),
						parseECField(o.getEcinternationalorganizationeurinterests()),
						parseECField(o.getEcinternationalorganization()),
						parseECField(o.getEcenterprise()),
						parseECField(o.getEcsmevalidated()),
						parseECField(o.getEcnutscode()));
				},
				Encoders.bean(OrgSimRel.class))
			.map(
				(MapFunction<OrgSimRel, Tuple2<String, OrgSimRel>>) o -> new Tuple2<>(o.getLocal_id(), o),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(OrgSimRel.class)));

		return relations2
			.joinWith(entities, relations2.col("_1").equalTo(entities.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple2<String, OrgSimRel>, Tuple2<String, Organization>>, OrgSimRel>) r -> {
					OrgSimRel orgSimRel = r._1()._2();
					orgSimRel.setLocal_id(r._2()._2().getOriginalId().get(0));
					return orgSimRel;
				},
				Encoders.bean(OrgSimRel.class));

	}

	// Sort IDs basing on the type. Priority: 1) openorgs, 2)corda, 3)alphabetic
	public static List<String> sortIds(List<String> ids) {
		ids.sort(DedupUtility::compareOpenOrgIds);
		return ids;
	}

	public static Dataset<OrgSimRel> createRelationsFromScratch(
		final SparkSession spark,
		final String mergeRelsPath,
		final String entitiesPath) {

		// <id, json_entity>
		Dataset<Tuple2<String, Organization>> entities = spark
			.read()
			.textFile(entitiesPath)
			.map(
				(MapFunction<String, Tuple2<String, Organization>>) it -> {
					Organization entity = OBJECT_MAPPER.readValue(it, Organization.class);
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Organization.class)));

		Dataset<Tuple2<String, String>> relations = spark
			.createDataset(
				spark
					.read()
					.load(mergeRelsPath)
					.as(Encoders.bean(Relation.class))
					.where("relClass == 'merges'")
					.toJavaRDD()
					.mapToPair(r -> new Tuple2<>(r.getSource(), r.getTarget()))
					.groupByKey()
					.flatMap(g -> {
						List<Tuple2<String, String>> rels = new ArrayList<>();
						for (String id1 : g._2()) {
							for (String id2 : g._2()) {
								if (!id1.equals(id2) && id1.contains(DedupUtility.OPENORGS_ID_PREFIX)
									&& !id2.contains("openorgsmesh")) {
									rels.add(new Tuple2<>(id1, id2));
								}
							}
						}
						return rels.iterator();
					})
					.rdd(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

		Dataset<Tuple2<String, OrgSimRel>> relations2 = relations // <openorgs, corda>
			.joinWith(entities, relations.col("_2").equalTo(entities.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Organization>>, OrgSimRel>) r -> new OrgSimRel(
					r._1()._1(),
					r._2()._2().getOriginalId().get(0),
					r._2()._2().getLegalname() != null ? r._2()._2().getLegalname().getValue() : "",
					r._2()._2().getLegalshortname() != null ? r._2()._2().getLegalshortname().getValue() : "",
					r._2()._2().getCountry() != null ? r._2()._2().getCountry().getClassid() : "",
					r._2()._2().getWebsiteurl() != null ? r._2()._2().getWebsiteurl().getValue() : "",
					r._2()._2().getCollectedfrom().get(0).getValue(),
					GROUP_PREFIX + r._1()._1(),
					structuredPropertyListToString(r._2()._2().getPid()),
					parseECField(r._2()._2().getEclegalbody()),
					parseECField(r._2()._2().getEclegalperson()),
					parseECField(r._2()._2().getEcnonprofit()),
					parseECField(r._2()._2().getEcresearchorganization()),
					parseECField(r._2()._2().getEchighereducation()),
					parseECField(r._2()._2().getEcinternationalorganizationeurinterests()),
					parseECField(r._2()._2().getEcinternationalorganization()),
					parseECField(r._2()._2().getEcenterprise()),
					parseECField(r._2()._2().getEcsmevalidated()),
					parseECField(r._2()._2().getEcnutscode())),
				Encoders.bean(OrgSimRel.class))
			.map(
				(MapFunction<OrgSimRel, Tuple2<String, OrgSimRel>>) o -> new Tuple2<>(o.getLocal_id(), o),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(OrgSimRel.class)));

		return relations2
			.joinWith(entities, relations2.col("_1").equalTo(entities.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple2<String, OrgSimRel>, Tuple2<String, Organization>>, OrgSimRel>) r -> {
					OrgSimRel orgSimRel = r._1()._2();
					orgSimRel.setLocal_id(r._2()._2().getOriginalId().get(0));
					return orgSimRel;
				},
				Encoders.bean(OrgSimRel.class));

	}

}
