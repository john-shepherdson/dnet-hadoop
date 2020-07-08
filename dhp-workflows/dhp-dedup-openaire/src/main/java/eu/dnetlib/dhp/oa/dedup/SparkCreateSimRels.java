
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
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.FieldListImpl;
import eu.dnetlib.pace.model.FieldValueImpl;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

public class SparkCreateSimRels extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkCreateSimRels.class);

	public SparkCreateSimRels(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
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
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf
			.registerKryoClasses(
				new Class[] {
					MapDocument.class, FieldListImpl.class, FieldValueImpl.class, Block.class
				});

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

			final String entity = dedupConf.getWf().getEntityType();
			final String subEntity = dedupConf.getWf().getSubEntityValue();
			log.info("Creating simrels for: '{}'", subEntity);

			final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity);
			removeOutputDir(spark, outputPath);

			JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

			JavaPairRDD<String, MapDocument> mapDocuments = sc
				.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
				.repartition(10000)
				.mapToPair(
					(PairFunction<String, String, MapDocument>) s -> {
						MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
						return new Tuple2<>(d.getIdentifier(), d);
					});

			// create blocks for deduplication
			JavaPairRDD<String, Block> blocks = Deduper
				.createSortedBlocks(mapDocuments, dedupConf)
				.repartition(10000);

			// create relations by comparing only elements in the same group
			Deduper
				.computeRelations(sc, blocks, dedupConf)
				.map(t -> createSimRel(t._1(), t._2(), entity))
				.repartition(10000)
				.map(r -> OBJECT_MAPPER.writeValueAsString(r))
				.saveAsTextFile(outputPath);

			// save the simrel in the workingdir
			/*
			 * spark .createDataset(relations.rdd(), Encoders.bean(Relation.class)) .write() .mode(SaveMode.Append)
			 * .save(outputPath);
			 */
		}
	}

	private Relation createSimRel(String source, String target, String entity) {
		final Relation r = new Relation();
		r.setSource(source);
		r.setTarget(target);
		r.setSubRelType("dedupSimilarity");
		r.setRelClass("isSimilarTo");
		r.setDataInfo(new DataInfo());

		switch (entity) {
			case "result":
				r.setRelType("resultResult");
				break;
			case "organization":
				r.setRelType("organizationOrganization");
				break;
			default:
				throw new IllegalArgumentException("unmanaged entity type: " + entity);
		}
		return r;
	}
}
