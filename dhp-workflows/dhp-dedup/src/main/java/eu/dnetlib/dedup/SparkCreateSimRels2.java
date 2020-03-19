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

public class SparkCreateSimRels2 implements Serializable {

    private static final Log log = LogFactory.getLog(SparkCreateSimRels2.class);

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/createSimRels_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateSimRels2().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws ISLookUpException, DocumentException {

        //read oozie parameters
        final String graphBasePath = parser.get("graphBasePath");
        final String rawSet = parser.get("rawSet");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");

        try (SparkSession spark = getSparkSession(parser)) {
            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            //create empty sequenceFile for the accumulation
            JavaRDD<Tuple2<Text,Text>> simRel = sc.emptyRDD();

            //for each dedup configuration
            for (DedupConfig dedupConf: DedupUtility.getConfigurations(isLookUpUrl, actionSetId)) {
                final String entity = dedupConf.getWf().getEntityType();
                final String subEntity = dedupConf.getWf().getSubEntityValue();

                JavaPairRDD<String, MapDocument> mapDocument = sc.textFile(graphBasePath + "/" + subEntity)
                        .mapToPair(s -> {
                            MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(dedupConf, s);
                            return new Tuple2<>(d.getIdentifier(), d);
                        });

                //create blocks for deduplication
                JavaPairRDD<String, List<MapDocument>> blocks = Deduper.createsortedBlocks(sc, mapDocument, dedupConf);

                //create relations by comparing only elements in the same group
                final JavaPairRDD<String, String> dedupRels = Deduper.computeRelations2(sc, blocks, dedupConf);

                JavaRDD<Relation> relationsRDD = dedupRels.map(r -> createSimRel(r._1(), r._2(), entity));

                //save the simrel in the workingdir
                spark.createDataset(relationsRDD.rdd(), Encoders.bean(Relation.class)).write().mode("overwrite").save( DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity));

                //create atomic actions
                JavaRDD<Tuple2<Text, Text>> newSimRels = relationsRDD
                        .map(this::createSequenceFileRow);

                simRel = simRel.union(newSimRels);
            }

            simRel.mapToPair(r -> r)
                    .saveAsHadoopFile(rawSet, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
        }

    }

    public Tuple2<Text, Text> createSequenceFileRow(Relation relation) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        String id = relation.getSource() + "@" + relation.getRelClass() + "@" + relation.getTarget();
        //TODO do be replaced by the new implementation of AtomicAction
        AtomicAction aa = new AtomicAction("rawSet", new Agent("agentId", "agentName", Agent.AGENT_TYPE.service), relation.getSource(), relation.getRelClass(), relation.getTarget(), new ObjectMapper().writeValueAsString(relation).getBytes());

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
                .appName(SparkCreateSimRels2.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }

}
