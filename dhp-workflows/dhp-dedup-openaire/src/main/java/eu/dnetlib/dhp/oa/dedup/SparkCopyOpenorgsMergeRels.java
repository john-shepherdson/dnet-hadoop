
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class SparkCopyOpenorgsMergeRels extends AbstractSparkAction {
	private static final Logger log = LoggerFactory.getLogger(SparkCopyOpenorgsMergeRels.class);

	public SparkCopyOpenorgsMergeRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgsMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkCopyOpenorgsMergeRels(parser, getSparkSession(conf))
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

		log.info("Copying OpenOrgs Merge Rels");

		final String outputPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, "organization");

		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		// collect organization merge relations from openorgs database
		JavaRDD<Relation> mergeRelsRDD = spark
			.read()
			.textFile(relationPath)
			.map(patchRelFn(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(this::isOpenorgs) // take only openorgs relations
			.filter(this::isMergeRel); // take merges and isMergedIn relations

		log.info("Number of Openorgs Merge Relations collected: {}", mergeRelsRDD.count());

		final Dataset<Relation> relations = spark
			.createDataset(
				mergeRelsRDD.rdd(),
				Encoders.bean(Relation.class));

		saveParquet(relations, outputPath, SaveMode.Append);
	}

	private boolean isMergeRel(Relation rel) {
		return (rel.getRelClass().equals(ModelConstants.MERGES)
			|| rel.getRelClass().equals(ModelConstants.IS_MERGED_IN))
			&& rel.getRelType().equals(ModelConstants.ORG_ORG_RELTYPE)
			&& rel.getSubRelType().equals(ModelConstants.DEDUP);
	}
}
