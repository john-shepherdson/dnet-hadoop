
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
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

//copy simrels (verified) from relation to the workdir in order to make them available for the deduplication
public class SparkCopyOpenorgsSimRels extends AbstractSparkAction {
	private static final Logger log = LoggerFactory.getLogger(SparkCopyOpenorgsSimRels.class);

	public SparkCopyOpenorgsSimRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgsSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkCopyOpenorgsSimRels(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws DocumentException, IOException, ISLookUpException {

		// read oozie parameters
		final String graphBasePath = parser.get("graphBasePath");
		log.info("graphBasePath: '{}'", graphBasePath);

		final String actionSetId = parser.get("actionSetId");
		log.info("actionSetId:   '{}'", actionSetId);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath:   '{}'", workingPath);

		final int numPartitions = Optional
			.ofNullable(parser.get("numPartitions"))
			.map(Integer::valueOf)
			.orElse(NUM_PARTITIONS);
		log.info("numPartitions: '{}'", numPartitions);

		log.info("Copying OpenOrgs SimRels");

		final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, "organization");

		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		Dataset<Relation> rawRels = spark
			.read()
			.textFile(relationPath)
			.map(parseRelFn(), Encoders.bean(Relation.class))
			.filter(this::filterOpenorgsRels);

		saveParquet(rawRels, outputPath, SaveMode.Append);

		log.info("Copied {} Similarity Relations", rawRels.count());
	}

	private boolean filterOpenorgsRels(Relation rel) {
		return rel.getRelClass().equals(Relation.RELCLASS.isSimilarTo)
			&& rel.getRelType().equals(Relation.RELTYPE.organizationOrganization)
			&& rel.getSubRelType().equals(Relation.SUBRELTYPE.dedup) && isOpenorgs(rel);
	}

}
