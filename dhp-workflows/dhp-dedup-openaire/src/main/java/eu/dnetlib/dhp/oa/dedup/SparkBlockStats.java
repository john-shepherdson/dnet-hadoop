
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.model.BlockStats;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkDeduper;

public class SparkBlockStats extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkBlockStats.class);

	public SparkBlockStats(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkBlockStats.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createBlockStats_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();

		new SparkBlockStats(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	public Long computeComparisons(Long blockSize, Long slidingWindowSize) {

		if (slidingWindowSize >= blockSize)
			return (slidingWindowSize * (slidingWindowSize - 1)) / 2;
		else {
			return (blockSize - slidingWindowSize + 1) * (slidingWindowSize * (slidingWindowSize - 1)) / 2;
		}
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws DocumentException, IOException, ISLookUpException, SAXException {

		// read oozie parameters
		final String graphBasePath = parser.get("graphBasePath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");
		final int numPartitions = Optional
			.ofNullable(parser.get("numPartitions"))
			.map(Integer::valueOf)
			.orElse(NUM_PARTITIONS);

		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		// for each dedup configuration
		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {

			final String subEntity = dedupConf.getWf().getSubEntityValue();
			log.info("Creating blockstats for: '{}'", subEntity);

			final String outputPath = DedupUtility.createBlockStatsPath(workingPath, actionSetId, subEntity);
			removeOutputDir(spark, outputPath);

			JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

			SparkDeduper deduper = new SparkDeduper(dedupConf);

			Dataset<Row> simRels = spark
				.read()
				.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.transform(deduper.model().parseJsonDataset())
				.transform(deduper.filterAndCleanup())
				.transform(deduper.generateClustersWithCollect())
				.filter(functions.size(new Column("block")).geq(1));

			simRels.map((MapFunction<Row, BlockStats>) row -> {
				Collection<Row> mapDocuments = row.getList(row.fieldIndex("block"));

				/*
				 * List<Row> mapDocuments = documents .stream() .sorted( new
				 * RowDataOrderingComparator(deduper.model().orderingFieldPosition(),
				 * deduper.model().identityFieldPosition())) .limit(dedupConf.getWf().getQueueMaxSize())
				 * .collect(Collectors.toList());
				 */

				return new BlockStats(
					row.getString(row.fieldIndex("key")),
					(long) mapDocuments.size(),
					computeComparisons(
						(long) mapDocuments.size(), (long) dedupConf.getWf().getSlidingWindowSize()));
			}, Encoders.bean(BlockStats.class))
				.write()
				.mode(SaveMode.Overwrite)
				.save(outputPath);
		}
	}
}
