
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.model.Block;
import eu.dnetlib.dhp.oa.dedup.model.BlockStats;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.FieldListImpl;
import eu.dnetlib.pace.model.FieldValueImpl;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class SparkBlockStats extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateSimRels.class);

	public SparkBlockStats(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createBlockStats_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();

		new SparkCreateSimRels(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService)
		throws DocumentException, IOException, ISLookUpException {

		// read oozie parameters
		final String graphBasePath = parser.get("graphBasePath");
		final String isLookUpUrl = parser.get("isLookUpUrl");
		final String actionSetId = parser.get("actionSetId");
		final String workingPath = parser.get("workingPath");

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

			JavaPairRDD<String, MapDocument> mapDocuments = sc
				.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.mapToPair(
					(PairFunction<String, String, MapDocument>) s -> {
						MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
						return new Tuple2<>(d.getIdentifier(), d);
					});

			// create blocks for deduplication
			JavaPairRDD<String, Block> blocks = Deduper.createSortedBlocks(mapDocuments, dedupConf);

			JavaRDD<BlockStats> blockStats = blocks
				.map(
					b -> new BlockStats(
						b._1(),
						(long) b._2().getDocuments().size(),
						computeComparisons(
							(long) b._2().getDocuments().size(), (long) dedupConf.getWf().getSlidingWindowSize())));

			// save the blockstats in the workingdir
			spark
				.createDataset(blockStats.rdd(), Encoders.bean(BlockStats.class))
				.write()
				.mode(SaveMode.Overwrite)
				.save(outputPath);
		}
	}

	public Long computeComparisons(Long blockSize, Long slidingWindowSize) {

		if (slidingWindowSize >= blockSize)
			return (slidingWindowSize * (slidingWindowSize - 1)) / 2;
		else {
			return (blockSize - slidingWindowSize + 1) * (slidingWindowSize * (slidingWindowSize - 1)) / 2;
		}
	}
}
