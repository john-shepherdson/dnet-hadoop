package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
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
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class SparkCreateSimRels extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkCreateSimRels.class);

    public SparkCreateSimRels(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateSimRels(parser, getSparkSession(parser)).run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService) throws DocumentException, IOException, ISLookUpException {

        //read oozie parameters
        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");

        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("isLookUpUrl:   '{}'", isLookUpUrl);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //for each dedup configuration
        for (DedupConfig dedupConf: getConfigurations(isLookUpService, actionSetId)) {

            final String entity = dedupConf.getWf().getEntityType();
            final String subEntity = dedupConf.getWf().getSubEntityValue();
            log.info("Creating simrels for: '{}'", subEntity);

            final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity);
            removeOutputDir(spark, outputPath);

            JavaPairRDD<String, MapDocument> mapDocument = sc.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
                    .mapToPair((PairFunction<String, String, MapDocument>)  s -> {
                        MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
                        return new Tuple2<>(d.getIdentifier(), d);
                    });

            //create blocks for deduplication
            JavaPairRDD<String, List<MapDocument>> blocks = Deduper.createSortedBlocks(sc, mapDocument, dedupConf);

            //create relations by comparing only elements in the same group
            final JavaPairRDD<String, String> dedupRels = Deduper.computeRelations(sc, blocks, dedupConf);

            JavaRDD<Relation> relationsRDD = dedupRels.map(r -> createSimRel(r._1(), r._2(), entity));

            //save the simrel in the workingdir
            spark.createDataset(relationsRDD.rdd(), Encoders.bean(Relation.class))
                    .write()
                    .mode(SaveMode.Append)
                    .save(outputPath);
        }
    }

    public Relation createSimRel(String source, String target, String entity) {
        final Relation r = new Relation();
        r.setSource(source);
        r.setTarget(target);
        r.setSubRelType("dedupSimilarity");
        r.setRelClass("isSimilarTo");
        r.setDataInfo(new DataInfo());

        switch(entity){
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
