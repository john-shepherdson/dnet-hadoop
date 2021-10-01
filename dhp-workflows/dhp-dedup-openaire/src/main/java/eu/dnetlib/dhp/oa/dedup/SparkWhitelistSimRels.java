package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.model.Block;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;
import scala.Tuple3;

public class SparkWhitelistSimRels extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkCreateSimRels.class);

    private static final String WHITELIST_SEPARATOR = "####";

    public SparkWhitelistSimRels(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkCreateSimRels.class
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

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //file format: source####target
        Dataset<Tuple2<String, String>> whiteListRels = spark.createDataset(sc
                        .textFile(whiteListPath)
                        //check if the line is in the correct format: id1####id2
                        .filter(s -> s.contains(WHITELIST_SEPARATOR) && s.split(WHITELIST_SEPARATOR).length == 2)
                        .map(s -> new Tuple2<>(s.split(WHITELIST_SEPARATOR)[0], s.split(WHITELIST_SEPARATOR)[1]))
                        .rdd(),
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        // for each dedup configuration
        for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {

            final String entity = dedupConf.getWf().getEntityType();
            final String subEntity = dedupConf.getWf().getSubEntityValue();
            log.info("Adding whitelist simrels for: '{}'", subEntity);

            final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity);

            Dataset<Tuple2<String, String>> entities = spark.createDataset(sc
                    .textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
                    .repartition(numPartitions)
                    .mapToPair(
                            (PairFunction<String, String, String>) s -> {
                                MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
                                return new Tuple2<>(d.getIdentifier(), "present");
                            })
                    .rdd(),
                    Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

            Dataset<Tuple2<String, String>> whiteListRels1 = whiteListRels
                    .joinWith(entities, whiteListRels.col("_1").equalTo(entities.col("_1")), "inner")
                    .map((MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, Tuple2<String, String>>) Tuple2::_1, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

            Dataset<Tuple2<String, String>> whiteListRels2 = whiteListRels1
                    .joinWith(entities, whiteListRels1.col("_2").equalTo(entities.col("_1")), "inner")
                    .map((MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, Tuple2<String, String>>) Tuple2::_1, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

            Dataset<Relation> whiteListSimRels = whiteListRels2
                    .map((MapFunction<Tuple2<String, String>, Relation>)
                                    r -> createSimRel(r._1(), r._2(), entity),
                                    Encoders.bean(Relation.class)
                    );

            saveParquet(whiteListSimRels, outputPath, SaveMode.Append);
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
