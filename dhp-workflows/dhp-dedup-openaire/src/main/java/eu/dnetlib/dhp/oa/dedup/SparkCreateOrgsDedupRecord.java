
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

public class SparkCreateOrgsDedupRecord extends AbstractSparkAction {
	private static final Logger log = LoggerFactory.getLogger(SparkCreateOrgsDedupRecord.class);

	public SparkCreateOrgsDedupRecord(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateOrgsDedupRecord.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgs_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkCreateOrgsDedupRecord(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws DocumentException, IOException, ISLookUpException {

		// read oozie parameters
		final String graphBasePath = parser.get("graphBasePath");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");
		final int numPartitions = Optional
			.ofNullable(parser.get("numPartitions"))
			.map(Integer::valueOf)
			.orElse(NUM_PARTITIONS);

		log.info("numPartitions: '{}'", numPartitions);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		log.info("Copying organization dedup records to the working dir");

		final String outputPath = DedupUtility.createDedupRecordPath(workingPath, actionSetId, "organization");

		final String entityPath = DedupUtility.createEntityPath(graphBasePath, "organization");

		final String mergeRelsPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, "organization");

		save(rootOrganization(spark, entityPath, mergeRelsPath), outputPath, SaveMode.Overwrite);

	}

	public static Dataset<Organization> rootOrganization(
		final SparkSession spark,
		final String entitiesInputPath,
		final String mergeRelsPath) {

		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaPairRDD<String, Organization> entities = sc
			.textFile(entitiesInputPath)
			.map(it -> OBJECT_MAPPER.readValue(it, Organization.class))
			.mapToPair(o -> new Tuple2<>(o.getId(), o));

		log.info("Number of organization entities processed: {}", entities.count());

		// collect root ids (ids in the source of 'merges' relations
		JavaPairRDD<String, String> roots = spark
			.read()
			.load(mergeRelsPath)
			.as(Encoders.bean(Relation.class))
			.where("relClass == 'merges'")
			.map(
				(MapFunction<Relation, Tuple2<String, String>>) r -> new Tuple2<>(r.getSource(), "root"),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
			.toJavaRDD()
			.mapToPair(t -> t)
			.distinct();

		Dataset<Organization> rootOrgs = spark
			.createDataset(
				entities
					.leftOuterJoin(roots)
					.filter(e -> e._2()._2().isPresent()) // if it has been joined with 'root' then it's a root record
					.map(e -> e._2()._1())
					.rdd(),
				Encoders.bean(Organization.class));

		log.info("Number of Root organization: {}", entities.count());

		return rootOrgs;
	}

}
