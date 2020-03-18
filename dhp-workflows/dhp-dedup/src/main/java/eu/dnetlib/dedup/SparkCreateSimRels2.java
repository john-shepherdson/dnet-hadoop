package eu.dnetlib.dedup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
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
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

public class SparkCreateSimRels2 implements Serializable {

    private static final Log log = LogFactory.getLog(SparkCreateSimRels2.class);

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/dedup_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateSimRels2().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws ISLookUpException, DocumentException {

        //read oozie parameters
        final String rawGraphBasePath = parser.get("rawGraphBasePath");
        final String rawSet = parser.get("rawSet");
        final String agentId = parser.get("agentId");
        final String agentName = parser.get("agentName");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");

        try (SparkSession spark = getSparkSession(parser)) {
            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            //create empty sequenceFile for the accumulation
            JavaRDD<Tuple2<Text,Text>> simRel = sc.emptyRDD();

            //for each dedup configuration
            for (DedupConfig dedupConf: getConfigurations(isLookUpUrl, actionSetId)) {
                final String entity = dedupConf.getWf().getEntityType();
                final String subEntity = dedupConf.getWf().getSubEntityValue();

                JavaPairRDD<String, MapDocument> mapDocument = sc.textFile(rawGraphBasePath + "/" + subEntity)
                        .mapToPair(s -> {
                            MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
                            return new Tuple2<>(d.getIdentifier(), d);
                        });

                //create blocks for deduplication
                JavaPairRDD<String, List<MapDocument>> blocks = Deduper.createsortedBlocks(sc, mapDocument, dedupConf);

                //create relations by comparing only elements in the same group
                final JavaPairRDD<String, String> dedupRels = Deduper.computeRelations2(sc, blocks, dedupConf);

                JavaRDD<Relation> relationsRDD = dedupRels.map(r -> createSimRel(r._1(), r._2()));

                //create atomic actions
                JavaRDD<Tuple2<Text, Text>> newSimRels = relationsRDD
                        .mapToPair(rel ->
                                new Tuple2<>(
                                        createActionId(rel.getSource(), rel.getTarget(), entity), //TODO update the type, maybe take it from the configuration?
                                        new AtomicAction(rawSet, new Agent(agentId, agentName, Agent.AGENT_TYPE.service), rel.getSource(), "isSimilarTo", rel.getTarget(), new ObjectMapper().writeValueAsString(rel).getBytes())))
                        .map(aa -> new Tuple2<>(aa._1(), transformAction(aa._2())));

                simRel = simRel.union(newSimRels);

            }

            simRel.mapToPair(r -> r)
                    .saveAsHadoopFile(rawSet, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);

        }

    }

    public Text createActionId(String source, String target, String entity) {

        String type = "";

        switch(entity){
            case "result":
                type = "resultResult_dedupSimilarity_isSimilarTo";
                break;
            case "organization":
                type = "organizationOrganization_dedupSimilarity_isSimilarTo";
                break;
            default:
                break;
        }

        String id = source + "@" + type + "@" + target;

        return new Text(id);
    }

    public Text transformAction(AtomicAction aa) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        return new Text(mapper.writeValueAsString(aa));
    }

    public Relation createSimRel(String source, String target){
        final Relation r = new Relation();
        r.setSource(source);
        r.setTarget(target);
        r.setRelClass("isSimilarTo");
        return r;
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkCreateSimRels2.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }

    public List<DedupConfig> getConfigurations(String isLookUpUrl, String orchestrator) throws ISLookUpException, DocumentException {
        final ISLookUpService isLookUpService = ISLookupClientFactory.getLookUpService(isLookUpUrl);

        final String xquery = String.format("/RESOURCE_PROFILE[.//DEDUPLICATION/ACTION_SET/@id = '%s']", orchestrator);
        log.info("loading dedup orchestration: " + xquery);

        String orchestratorProfile = isLookUpService.getResourceProfileByQuery(xquery);

        final Document doc = new SAXReader().read(new StringReader(orchestratorProfile));

        final String actionSetId = doc.valueOf("//DEDUPLICATION/ACTION_SET/@id");
        final List<DedupConfig> configurations = new ArrayList<>();

        for (final Object o : doc.selectNodes("//SCAN_SEQUENCE/SCAN")) {
            configurations.add(loadConfig(isLookUpService, actionSetId, o));
        }

        return configurations;

    }

    private DedupConfig loadConfig(final ISLookUpService isLookUpService, final String actionSetId, final Object o)
            throws ISLookUpException {
        final Element s = (Element) o;
        final String configProfileId = s.attributeValue("id");
        final String conf =
                isLookUpService.getResourceProfileByQuery(String.format(
                        "for $x in /RESOURCE_PROFILE[.//RESOURCE_IDENTIFIER/@value = '%s'] return $x//DEDUPLICATION/text()",
                        configProfileId));
        log.debug("loaded dedup configuration from IS profile: " + conf);
        final DedupConfig dedupConfig = DedupConfig.load(conf);
        dedupConfig.getWf().setConfigurationId(actionSetId);
        return dedupConfig;
    }

}
