
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.model.OrgSimRel;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class SparkPrepareNewOrgs extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkPrepareNewOrgs.class);

	public SparkPrepareNewOrgs(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkPrepareNewOrgs.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/prepareNewOrgs_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkPrepareNewOrgs(parser, getSparkSession(conf))
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

		final String apiUrl = Optional
			.ofNullable(parser.get("apiUrl"))
			.orElse("");

		final String dbUrl = parser.get("dbUrl");
		final String dbTable = parser.get("dbTable");
		final String dbUser = parser.get("dbUser");
		final String dbPwd = parser.get("dbPwd");

		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);
		log.info("numPartitions: '{}'", numConnections);
		log.info("apiUrl:        '{}'", apiUrl);
		log.info("dbUrl:         '{}'", dbUrl);
		log.info("dbUser:        '{}'", dbUser);
		log.info("table:         '{}'", dbTable);
		log.info("dbPwd:         '{}'", "xxx");

		final String entityPath = DedupUtility.createEntityPath(graphBasePath, "organization");
		final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, "organization");
		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		Dataset<OrgSimRel> newOrgs = createNewOrgs(spark, mergeRelPath, relationPath, entityPath);

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);

		log.info("Number of New Organization created: '{}'", newOrgs.count());

		newOrgs
			.repartition(numConnections)
			.write()
			.mode(SaveMode.Append)
			.jdbc(dbUrl, dbTable, connectionProperties);

		// TODO de-comment once finished
//		if (!apiUrl.isEmpty())
//			updateSimRels(apiUrl);

	}

	public static Dataset<OrgSimRel> createNewOrgs(
		final SparkSession spark,
		final String mergeRelsPath,
		final String relationPath,
		final String entitiesPath) {

		// collect diffrels from the raw graph relations: <other id, "diffRel">
		JavaPairRDD<String, String> diffRels = spark
			.read()
			.textFile(relationPath)
			.map(patchRelFn(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(r -> filterRels(r, "organization"))
			// take the worst id of the diffrel: <other id, "diffRel">
			.mapToPair(rel -> {
				if (compareIds(rel.getSource(), rel.getTarget()) > 0)
					return new Tuple2<>(rel.getSource(), "diffRel");
				else
					return new Tuple2<>(rel.getTarget(), "diffRel");
			})
			.distinct();
		log.info("Number of DiffRels collected: '{}'", diffRels.count());

		// collect entities: <id, json_entity>
		Dataset<Tuple2<String, Organization>> entities = spark
			.read()
			.textFile(entitiesPath)
			.map(
				(MapFunction<String, Tuple2<String, Organization>>) it -> {
					Organization entity = OBJECT_MAPPER.readValue(it, Organization.class);
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Organization.class)));

		// collect mergerels and remove ids in the diffrels
		Dataset<Tuple2<String, String>> openorgsRels = spark
			.createDataset(
				spark
					.read()
					.load(mergeRelsPath)
					.as(Encoders.bean(Relation.class))
					.where("relClass == 'isMergedIn'")
					.toJavaRDD()
					.mapToPair(r -> new Tuple2<>(r.getSource(), r.getTarget())) // <id, dedup_id>
					.leftOuterJoin(diffRels) // <target, "diffRel">
					.filter(rel -> !rel._2()._2().isPresent())
					.mapToPair(rel -> new Tuple2<>(rel._1(), rel._2()._1()))
					.rdd(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
		log.info("Number of Openorgs Relations loaded: '{}'", openorgsRels.count());

		return entities
			.joinWith(openorgsRels, entities.col("_1").equalTo(openorgsRels.col("_1")), "left")
			.filter((FilterFunction<Tuple2<Tuple2<String, Organization>, Tuple2<String, String>>>) t -> t._2() == null)
			// take entities not in mergerels (they are single entities, therefore are new orgs)
			.filter(
				(FilterFunction<Tuple2<Tuple2<String, Organization>, Tuple2<String, String>>>) t -> !t
					._1()
					._1()
					.contains("openorgs"))
			// exclude openorgs, don't need to propose them as new orgs
			.map(
				(MapFunction<Tuple2<Tuple2<String, Organization>, Tuple2<String, String>>, OrgSimRel>) r -> new OrgSimRel(
					"",
					r._1()._2().getOriginalId().get(0),
					r._1()._2().getLegalname() != null ? r._1()._2().getLegalname().getValue() : "",
					r._1()._2().getLegalshortname() != null ? r._1()._2().getLegalshortname().getValue() : "",
					r._1()._2().getCountry() != null ? r._1()._2().getCountry().getClassid() : "",
					r._1()._2().getWebsiteurl() != null ? r._1()._2().getWebsiteurl().getValue() : "",
					r._1()._2().getCollectedfrom().get(0).getValue(),
					"",
					structuredPropertyListToString(r._1()._2().getPid())),
				Encoders.bean(OrgSimRel.class));

	}

	private static String updateSimRels(final String apiUrl) throws IOException {

		log.info("Updating simrels on the portal");

		final HttpGet req = new HttpGet(apiUrl);
		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		}
	}

	private static MapFunction<String, Relation> patchRelFn() {
		return value -> {
			final Relation rel = OBJECT_MAPPER.readValue(value, Relation.class);
			if (rel.getDataInfo() == null) {
				rel.setDataInfo(new DataInfo());
			}
			return rel;
		};
	}

	public static int compareIds(String o1, String o2) {
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
	}

	private static boolean filterRels(Relation rel, String entityType) {

		switch (entityType) {
			case "result":
				if (rel.getRelClass().equals("isDifferentFrom") && rel.getRelType().equals("resultResult")
					&& rel.getSubRelType().equals("dedup"))
					return true;
				break;
			case "organization":
				if (rel.getRelClass().equals("isDifferentFrom") && rel.getRelType().equals("organizationOrganization")
					&& rel.getSubRelType().equals("dedup"))
					return true;
				break;
			default:
				return false;
		}
		return false;
	}

}
