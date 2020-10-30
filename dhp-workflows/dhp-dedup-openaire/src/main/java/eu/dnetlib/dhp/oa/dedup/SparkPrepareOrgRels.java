
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
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
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;
import scala.Tuple3;

public class SparkPrepareOrgRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateDedupRecord.class);

	public SparkPrepareOrgRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
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

		final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, "organization");
		final String entityPath = DedupUtility.createEntityPath(graphBasePath, "organization");

		Dataset<OrgSimRel> relations = createRelations(spark, mergeRelPath, entityPath);

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);

		relations
			.repartition(numConnections)
			.write()
			.mode(SaveMode.Overwrite)
			.jdbc(dbUrl, dbTable, connectionProperties);

	}

	public static Dataset<OrgSimRel> createRelations(
		final SparkSession spark,
		final String mergeRelsPath,
		final String entitiesPath) {

		Dataset<Tuple2<String, Organization>> entities = spark
			.read()
			.textFile(entitiesPath)
			.map(
				(MapFunction<String, Tuple2<String, Organization>>) it -> {
					Organization entity = OBJECT_MAPPER.readValue(it, Organization.class);
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Organization.class)));

		Dataset<Tuple3<String, String, String>> relations = spark
			.createDataset(
				spark
					.read()
					.load(mergeRelsPath)
					.as(Encoders.bean(Relation.class))
					.where("relClass == 'merges'")
					.toJavaRDD()
					.mapToPair(r -> new Tuple2<>(r.getSource(), r.getTarget()))
					.filter(t -> !t._2().contains("openorgsmesh"))
					.groupByKey()
					.map(g -> Lists.newArrayList(g._2()))
					.filter(l -> l.size() > 1)
					.flatMap(l -> {
						String groupId = "group::" + UUID.randomUUID();
						List<String> ids = sortIds(l);
						List<Tuple3<String, String, String>> rels = new ArrayList<>();

						for (String source : ids) {
							if (source.contains("openorgs____") || ids.indexOf(source) == 0)
								for (String target : ids) {
									rels.add(new Tuple3<>(source, target, groupId));
								}
						}
						return rels.iterator();
					})
					.rdd(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

		Dataset<Tuple2<String, OrgSimRel>> relations2 = relations // <openorgs, corda>
			.joinWith(entities, relations.col("_2").equalTo(entities.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple3<String, String, String>, Tuple2<String, Organization>>, OrgSimRel>) r -> new OrgSimRel(
					r._1()._1(),
					r._2()._2().getOriginalId().get(0),
					r._2()._2().getLegalname() != null ? r._2()._2().getLegalname().getValue() : "",
					r._2()._2().getLegalshortname() != null ? r._2()._2().getLegalshortname().getValue() : "",
					r._2()._2().getCountry() != null ? r._2()._2().getCountry().getClassid() : "",
					r._2()._2().getWebsiteurl() != null ? r._2()._2().getWebsiteurl().getValue() : "",
					r._2()._2().getCollectedfrom().get(0).getValue(),
					r._1()._3()),
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

	// select best ids from the list. Priority: 1) openorgs, 2)corda, 3)alphabetic
	public static List<String> sortIds(List<String> ids) {

		ids.sort((o1, o2) -> {

			if (o1.contains("openorgs____") && o2.contains("openorgs____"))
				return o1.compareTo(o2);
			if (o1.contains("corda") && o2.contains("corda"))
				return o1.compareTo(o2);

			if (o1.contains("openorgs____"))
				return -1;
			if (o2.contains("openorgs____"))
				return 1;

			if (o1.contains("corda"))
				return -1;
			if (o2.contains("corda"))
				return 1;

			return o1.compareTo(o2);
		});

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
								if (!id1.equals(id2))
									if (id1.contains("openorgs____") && !id2.contains("openorgsmesh"))
										rels.add(new Tuple2<>(id1, id2));
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
					"group::" + r._1()._1()),
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
