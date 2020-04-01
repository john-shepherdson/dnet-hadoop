package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import scala.Tuple2;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class SparkCreateSimRels implements Serializable {

    private static final Log log = LogFactory.getLog(SparkCreateSimRels.class);

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateSimRels().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws ISLookUpException, DocumentException {

        //read oozie parameters
        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");

        System.out.println(String.format("graphBasePath: '%s'", graphBasePath));
        System.out.println(String.format("isLookUpUrl: '%s'", isLookUpUrl));
        System.out.println(String.format("actionSetId: '%s'", actionSetId));
        System.out.println(String.format("workingPath: '%s'", workingPath));

        List<DedupConfig> configurations = getConfigurations(isLookUpUrl, actionSetId);
        System.out.println("configurations = " + configurations.size());

//        try (SparkSession spark = getSparkSession(parser)) {
//            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//
//            //for each dedup configuration
//            for (DedupConfig dedupConf: getConfigurations(isLookUpUrl, actionSetId)) {
//                final String entity = dedupConf.getWf().getEntityType();
//                final String subEntity = dedupConf.getWf().getSubEntityValue();
//
//                JavaPairRDD<String, MapDocument> mapDocument = sc.textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
//                        .mapToPair((PairFunction<String, String, MapDocument>)  s -> {
//                            MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
//                            return new Tuple2<String, MapDocument>(d.getIdentifier(), d);
//                        });
//
//                //create blocks for deduplication
//                JavaPairRDD<String, List<MapDocument>> blocks = Deduper.createsortedBlocks(sc, mapDocument, dedupConf);
//
//                //create relations by comparing only elements in the same group
//                final JavaPairRDD<String, String> dedupRels = Deduper.computeRelations2(sc, blocks, dedupConf);
//
//                JavaRDD<Relation> relationsRDD = dedupRels.map(r -> createSimRel(r._1(), r._2(), entity));
//
//                //save the simrel in the workingdir
//                spark.createDataset(relationsRDD.rdd(), Encoders.bean(Relation.class))
//                        .write()
//                        .mode("overwrite")
//                        .save(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity));
//            }
//        }
    }

    /**
     * Utility method used to create an atomic action from a Relation object
     * @param relation input relation
     * @return A tuple2 with [id, json serialization of the atomic action]
     * @throws JsonProcessingException
     */
    public Tuple2<Text, Text> createSequenceFileRow(Relation relation) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        String id = relation.getSource() + "@" + relation.getRelClass() + "@" + relation.getTarget();
        AtomicAction<Relation> aa = new AtomicAction<>(Relation.class, relation);

        return new Tuple2<>(
                new Text(id),
                new Text(mapper.writeValueAsString(aa))
        );
    }

    public Relation createSimRel(String source, String target, String entity){
        final Relation r = new Relation();
        r.setSource(source);
        r.setTarget(target);

        switch(entity){
            case "result":
                r.setRelClass("resultResult_dedupSimilarity_isSimilarTo");
                break;
            case "organization":
                r.setRelClass("organizationOrganization_dedupSimilarity_isSimilarTo");
                break;
            default:
                r.setRelClass("isSimilarTo");
                break;
        }
        return r;
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .getOrCreate();
    }

    public List<DedupConfig> getConfigurations(String isLookUpUrl, String orchestrator) throws ISLookUpException, DocumentException {
        final ISLookUpService isLookUpService = ISLookupClientFactory.getLookUpService(isLookUpUrl);

        final String xquery = String.format("/RESOURCE_PROFILE[.//DEDUPLICATION/ACTION_SET/@id = '%s']", orchestrator);

        String orchestratorProfile = isLookUpService.getResourceProfileByQuery(xquery);

        final Document doc = new SAXReader().read(new StringReader(orchestratorProfile));

        final String actionSetId = doc.valueOf("//DEDUPLICATION/ACTION_SET/@id");
        final List<DedupConfig> configurations = new ArrayList<>();

        for (final Object o : doc.selectNodes("//SCAN_SEQUENCE/SCAN")) {
            configurations.add(loadConfig(isLookUpService, actionSetId, o));
        }

        return configurations;

    }

    public DedupConfig loadConfig(final ISLookUpService isLookUpService, final String actionSetId, final Object o)
            throws ISLookUpException {
        final Element s = (Element) o;
        final String configProfileId = s.attributeValue("id");
        final String conf =
                isLookUpService.getResourceProfileByQuery(String.format(
                        "for $x in /RESOURCE_PROFILE[.//RESOURCE_IDENTIFIER/@value = '%s'] return $x//DEDUPLICATION/text()",
                        configProfileId));
        final DedupConfig dedupConfig = DedupConfig.load(conf);
        dedupConfig.getWf().setConfigurationId(actionSetId);
        return dedupConfig;
    }

}
