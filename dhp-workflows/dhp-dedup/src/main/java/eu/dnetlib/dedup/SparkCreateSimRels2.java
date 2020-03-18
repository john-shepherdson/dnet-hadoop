package eu.dnetlib.dedup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
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
import scala.Tuple2;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SparkCreateSimRels2 implements Serializable {

    final static String CONF_SEPARATOR = "@@@";

    private static final Log log = LogFactory.getLog(SparkCreateSimRels2.class);

    public static List<DedupConfig> decompressConfs(String compressedConfs){

        return Arrays.stream(compressedConfs.split(CONF_SEPARATOR))
                .map(ArgumentApplicationParser::decompressValue)
                .map(DedupConfig::load)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/dedup_parameters.json")));

        parser.parseArgument(args);

        new SparkCreateSimRels2().run(parser, decompressConfs(parser.get("dedupConf")));
    }

    private void run(ArgumentApplicationParser parser, List<DedupConfig> dedupConfs) {

        //read oozie parameters
        final String sourcePath = parser.get("sourcePath");
        final String targetPath = parser.get("targetPath");
        final String rawSetName = parser.get("rawSet");
        final String agentId = parser.get("agentId");
        final String agentName = parser.get("agentName");

        try (SparkSession spark = getSparkSession(parser)) {
            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            //create empty sequenceFile for the accumulation
            JavaRDD<Tuple2<Text,Text>> simRel = sc.emptyRDD();

            //for each dedup configuration
            for (DedupConfig dedupConf: dedupConfs) {
                final String entity = dedupConf.getWf().getEntityType();

                JavaPairRDD<String, MapDocument> mapDocument = sc.textFile(sourcePath + "/" + entity)
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
                                        new AtomicAction(rawSetName, new Agent(agentId, agentName, Agent.AGENT_TYPE.service), rel.getSource(), "isSimilarTo", rel.getTarget(), new ObjectMapper().writeValueAsString(rel).getBytes())))
                        .map(aa -> new Tuple2<>(aa._1(), transformAction(aa._2())));

                simRel = simRel.union(newSimRels);

            }

            String targetDirectory = targetPath + "/" + rawSetName;

//            simRel.map(s -> s._1().toString()).saveAsTextFile(targetDirectory);

            simRel.mapToPair(r -> r)
                    .saveAsHadoopFile(targetDirectory, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);

        }

    }

    public Text createActionId(String source, String target, String type) {
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
//                .enableHiveSupport()
                .getOrCreate();
    }

}
