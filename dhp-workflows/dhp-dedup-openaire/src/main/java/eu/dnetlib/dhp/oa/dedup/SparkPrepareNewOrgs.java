
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class SparkPrepareNewOrgs extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateDedupRecord.class);

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

		Dataset<OrgSimRel> newOrgs = createNewOrgs(spark, mergeRelPath, entityPath);

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);

		newOrgs
			.repartition(numConnections)
			.write()
			.mode(SaveMode.Overwrite)
			.jdbc(dbUrl, dbTable, connectionProperties);

	}

	public static Dataset<OrgSimRel> createNewOrgs(
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

		Dataset<Tuple2<String, String>> mergerels = spark
			.createDataset(
				spark
					.read()
					.load(mergeRelsPath)
					.as(Encoders.bean(Relation.class))
					.where("relClass == 'isMergedIn'")
					.toJavaRDD()
					.mapToPair(r -> new Tuple2<>(r.getSource(), r.getTarget()))
					.rdd(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

		return entities
			.joinWith(mergerels, entities.col("_1").equalTo(mergerels.col("_1")), "left")
			.filter((FilterFunction<Tuple2<Tuple2<String, Organization>, Tuple2<String, String>>>) t -> t._2() == null)
			.filter(
				(FilterFunction<Tuple2<Tuple2<String, Organization>, Tuple2<String, String>>>) t -> !t
					._1()
					._1()
					.contains("openorgs"))
			.map(
				(MapFunction<Tuple2<Tuple2<String, Organization>, Tuple2<String, String>>, OrgSimRel>) r -> new OrgSimRel(
					"",
					r._1()._2().getOriginalId().get(0),
					r._1()._2().getLegalname() != null ? r._1()._2().getLegalname().getValue() : "",
					r._1()._2().getLegalshortname() != null ? r._1()._2().getLegalshortname().getValue() : "",
					r._1()._2().getCountry() != null ? r._1()._2().getCountry().getClassid() : "",
					r._1()._2().getWebsiteurl() != null ? r._1()._2().getWebsiteurl().getValue() : "",
					r._1()._2().getCollectedfrom().get(0).getValue()),
				Encoders.bean(OrgSimRel.class));

	}

}
