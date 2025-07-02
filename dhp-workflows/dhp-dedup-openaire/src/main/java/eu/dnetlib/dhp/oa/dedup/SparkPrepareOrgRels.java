package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.model.OrgSimRel;
import eu.dnetlib.dhp.oa.dedup.model.ParentChildRel;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

public class SparkPrepareOrgRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkPrepareOrgRels.class);

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
		final String dedupEventsTable = parser.get("dedupEventsTable");
		final String parentChildTable = parser.get("parentChildTable");
		final String dbUser = parser.get("dbUser");
		final String dbPwd = parser.get("dbPwd");

		log.info("graphBasePath:      '{}'", graphBasePath);
		log.info("isLookUpUrl:        '{}'", isLookUpUrl);
		log.info("actionSetId:        '{}'", actionSetId);
		log.info("workingPath:        '{}'", workingPath);
		log.info("numPartitions:      '{}'", numConnections);
		log.info("dbUrl:              '{}'", dbUrl);
		log.info("dbUser:             '{}'", dbUser);
		log.info("dedupEventsTable:   '{}'", dedupEventsTable);
		log.info("parentChildTable:   '{}'", parentChildTable);
		log.info("dbPwd:              '{}'", "xxx");

		final String organization = ModelSupport.getMainType(EntityType.organization);
		final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, organization);
		final String entityPath = DedupUtility.createEntityPath(graphBasePath, organization);
		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		// collect DiffRels from the raw graph relations: <<best id, other id>, "diffRel">
		JavaRDD<Tuple2<Tuple2<String, String>, String>> diffRels = OpenorgsUtility
			.collectRels(
				spark, relationPath, ModelConstants.IS_DIFFERENT_FROM, ModelConstants.ORG_ORG_RELTYPE,
				ModelConstants.DEDUP, true);
		log.info("Number of DiffRels collected: {}", diffRels.count());

		// collect ParentChildRels from the raw graph relations: <<best id, other id>, "parentChildRel">
		JavaRDD<Tuple2<Tuple2<String, String>, String>> parentChildRels = OpenorgsUtility
			.collectRels(
				spark, relationPath, ModelConstants.IS_PARENT_OF, ModelConstants.ORG_ORG_RELTYPE, ModelConstants.RELATIONSHIP,
				false);
		log.info("Number of Parent/Child Rels collected: {}", parentChildRels.count());

		// collect all the organizations
		Dataset<Tuple2<String, Organization>> entities = spark
			.read()
			.textFile(entityPath)
			.map(
				(MapFunction<String, Tuple2<String, Organization>>) it -> {
					Organization entity = OBJECT_MAPPER.readValue(it, Organization.class);
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Organization.class)));

		// process mergeRels: <source, target, group id>
		JavaRDD<Tuple3<String, String, String>> processedMergeRels = OpenorgsUtility
			.processMergeRels(spark, mergeRelPath, diffRels, parentChildRels).cache();

		// create parent/child suggestion relations: <parent(raw or representative), child (raw or representative)>
		Dataset<ParentChildRel> parentChildSuggestions = createParentChildSuggestions(
			spark, processedMergeRels, parentChildRels, entities).cache();

		// create duplicate suggestion relations: <best id, other id>
		Dataset<OrgSimRel> duplicatesSuggestions = createDuplicatesSuggestions(spark, processedMergeRels, entities).cache();

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);
		processedMergeRels.unpersist();

		// save dedup events into temporary table
		duplicatesSuggestions
			.repartition(numConnections)
			.write()
			.mode(SaveMode.Overwrite)
			.jdbc(dbUrl, dedupEventsTable, connectionProperties);
		duplicatesSuggestions.unpersist();

		// save parent/child relations into temporary table
		parentChildSuggestions
			.repartition(numConnections)
			.write()
			.mode(SaveMode.Overwrite)
			.jdbc(dbUrl, parentChildTable, connectionProperties);
		parentChildSuggestions.unpersist();
	}

	private static Dataset<ParentChildRel> createParentChildSuggestions(SparkSession spark,
		JavaRDD<Tuple3<String, String, String>> openorgsRels,
		JavaRDD<Tuple2<Tuple2<String, String>, String>> parentChildRels,
		Dataset<Tuple2<String, Organization>> entities) {

		JavaPairRDD<String, String> pcRels = parentChildRels
			.mapToPair(r -> r._1()); // <raw_parent, raw_child>

		JavaPairRDD<String, String> rawReprRels = openorgsRels
			.mapToPair(r -> new Tuple2<>(r._2(), r._1())); // <raw, repr>

		pcRels = pcRels
			.leftOuterJoin(rawReprRels) // <raw_parent, <raw_child, parent_repr>>
			.mapToPair(j -> new Tuple2<>(j._2()._1(), j._2()._2().orElse(j._1()))); // <raw_child, parent_repr>

		pcRels = pcRels
			.leftOuterJoin(rawReprRels) // <raw_child, <parent_repr, child_repr>>
			.mapToPair(j -> new Tuple2<>(j._2()._1(), j._2()._2().orElse(j._1()))); // <parent_repr, child_repr>

		JavaRDD<ParentChildRel> parentChildRelRDD = pcRels
			.join(entities.toJavaRDD().mapToPair(r -> new Tuple2<>(r._1(), r._2())))
			.mapToPair(
				r -> new Tuple2<>(r._2()._1(),
					Optional.ofNullable(r._2()._2().getOriginalId()).map(oid -> oid.get(0)).orElse(null)))
			.join(entities.toJavaRDD().mapToPair(r -> new Tuple2<>(r._1(), r._2())))
			.mapToPair(
				r -> new Tuple2<>(r._2()._1(),
					Optional.ofNullable(r._2()._2().getOriginalId()).map(oid -> oid.get(0)).orElse(null)))
			.filter(j -> !j._1().equals(j._2())) // remove self relations
			.flatMap(
				j -> Arrays
					.asList(
						new ParentChildRel(j._1(), j._2(), ModelConstants.IS_PARENT_OF),
						new ParentChildRel(j._2(), j._1(), ModelConstants.IS_CHILD_OF))
					.iterator())
			.distinct();

		return spark.createDataset(parentChildRelRDD.rdd(), Encoders.bean(ParentChildRel.class));
	}

	private static Dataset<OrgSimRel> createDuplicatesSuggestions(SparkSession spark,
		JavaRDD<Tuple3<String, String, String>> openorgsRels, Dataset<Tuple2<String, Organization>> entities) {
		// <best ID based on priority, ID, groupID>
		Dataset<Tuple3<String, String, String>> relations = spark
			.createDataset(
				openorgsRels.rdd(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

		// create OrgSimRels: <local_id, orgsimrel>
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
					orgSimRel
						.setLocal_id(
							Optional.ofNullable(r._2()._2().getOriginalId()).map(oid -> oid.get(0)).orElse(null));
					return orgSimRel;
				},
				Encoders.bean(OrgSimRel.class));
	}

}
