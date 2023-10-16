
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
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
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;

public class SparkWhitelistSimRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkWhitelistSimRels.class);

	private static final String WHITELIST_SEPARATOR = "####";

	public SparkWhitelistSimRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkWhitelistSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/whitelistSimRels_parameters.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		new SparkWhitelistSimRels(parser, getSparkSession(conf))
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
		final String whiteListPath = parser.get("whiteListPath");

		log.info("numPartitions: '{}'", numPartitions);
		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("isLookUpUrl:   '{}'", isLookUpUrl);
		log.info("actionSetId:   '{}'", actionSetId);
		log.info("workingPath:   '{}'", workingPath);
		log.info("whiteListPath: '{}'", whiteListPath);

		// file format: source####target
		Dataset<Row> whiteListRels = spark
			.read()
			.textFile(whiteListPath)
			.withColumn("pairs", functions.split(new Column("value"), WHITELIST_SEPARATOR))
			.filter(functions.size(new Column("pairs")).equalTo(2))
			.select(
				functions.element_at(new Column("pairs"), 1).as("from"),
				functions.element_at(new Column("pairs"), 2).as("to"));

		// for each dedup configuration
		for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {

			final String entity = dedupConf.getWf().getEntityType();
			final String subEntity = dedupConf.getWf().getSubEntityValue();
			log.info("Adding whitelist simrels for: '{}'", subEntity);

			final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity);

			// DFMapDocumentUtils.registerUDFs(spark, dedupConf);

			Dataset<Row> entities = spark
				.read()
				.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.repartition(numPartitions)
				.withColumn("id", functions.get_json_object(new Column("value"), dedupConf.getWf().getIdPath()));

			Dataset<Row> whiteListRels1 = whiteListRels
				.join(entities, entities.col("id").equalTo(whiteListRels.col("from")), "inner")
				.select("from", "to");

			Dataset<Row> whiteListRels2 = whiteListRels1
				.join(entities, whiteListRels1.col("to").equalTo(entities.col("id")), "inner")
				.select("from", "to");

			Dataset<Relation> whiteListSimRels = whiteListRels2
				.map(
					(MapFunction<Row, Relation>) r -> DedupUtility
						.createSimRel(r.getString(0), r.getString(1), entity),
					Encoders.bean(Relation.class));

			saveParquet(whiteListSimRels, outputPath, SaveMode.Append);
		}
	}

}
