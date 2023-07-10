
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.application.dedup.log.DedupLogModel;
import eu.dnetlib.dhp.application.dedup.log.DedupLogWriter;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkDedupConfig;

public class SparkCreateSimRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateSimRels.class);

	public SparkCreateSimRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
		spark.sparkContext().setLogLevel("WARN");
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkCreateSimRels(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
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

		log.info("numPartitions: '{}'", numPartitions);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);

		final String dfLogPath = parser.get("dataframeLog");
		final String runTag = Optional.ofNullable(parser.get("runTAG")).orElse("UNKNOWN");

		// for each dedup configuration
		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {
			final long start = System.currentTimeMillis();

			final String entity = dedupConf.getWf().getEntityType();
			final String subEntity = dedupConf.getWf().getSubEntityValue();
			log.info("Creating simrels for: '{}'", subEntity);

			final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity);
			removeOutputDir(spark, outputPath);

			JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

			SparkDedupConfig sparkConfig = new SparkDedupConfig(dedupConf, numPartitions);

			spark.udf().register("collect_sort_slice", sparkConfig.collectSortSliceUDAF());

			Dataset<?> simRels = spark
				.read()
				.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.transform(sparkConfig.modelExtractor()) // Extract fields from input json column according to model
				// definition
				.transform(sparkConfig.generateClustersWithWindows()) // generate <key,block> pairs according to
				// filters, clusters, and model
				// definition
				.transform(sparkConfig.processClusters()) // process blocks and emits <from,to> pairs of found
				// similarities
				.map(
					(MapFunction<Row, Relation>) t -> DedupUtility
						.createSimRel(t.getStruct(0).getString(0), t.getStruct(0).getString(1), entity),
					Encoders.bean(Relation.class));

			saveParquet(simRels, outputPath, SaveMode.Overwrite);
			final long end = System.currentTimeMillis();
			if (StringUtils.isNotBlank(dfLogPath)) {
				final DedupLogModel model = new DedupLogModel(runTag, dedupConf.toString(), subEntity, start, end,
					end - start);
				new DedupLogWriter(dfLogPath).appendLog(model, spark);

			}

		}
	}

}
