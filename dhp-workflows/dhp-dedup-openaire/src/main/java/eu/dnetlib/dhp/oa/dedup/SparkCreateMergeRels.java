package eu.dnetlib.dhp.oa.dedup;

import com.google.common.hash.Hashing;
import eu.dnetlib.dhp.oa.dedup.graph.ConnectedComponent;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.graph.GraphProcessor;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkCreateMergeRels extends AbstractSparkAction {

    private static final Log log = LogFactory.getLog(SparkCreateMergeRels.class);

    public SparkCreateMergeRels(ArgumentApplicationParser parser, SparkSession spark) throws Exception {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateMergeRels(parser, getSparkSession(parser)).run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService) throws ISLookUpException, DocumentException, IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");

        System.out.println(String.format("graphBasePath: '%s'", graphBasePath));
        System.out.println(String.format("isLookUpUrl:   '%s'", isLookUpUrl));
        System.out.println(String.format("actionSetId:   '%s'", actionSetId));
        System.out.println(String.format("workingPath:   '%s'", workingPath));

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        for (DedupConfig dedupConf: getConfigurations(isLookUpService, actionSetId)) {
            final String subEntity = dedupConf.getWf().getSubEntityValue();
            System.out.println(String.format("Creating mergerels for: '%s'", subEntity));

            final JavaPairRDD<Object, String> vertexes = sc.textFile(graphBasePath + "/" + subEntity)
                    .map(s -> MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), s))
                    .mapToPair((PairFunction<String, Object, String>)
                            s -> new Tuple2<Object, String>(getHashcode(s), s)
                    );

            final Dataset<Relation> similarityRelations = spark.read().load(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity)).as(Encoders.bean(Relation.class));
            final RDD<Edge<String>> edgeRdd = similarityRelations.javaRDD().map(it -> new Edge<>(getHashcode(it.getSource()), getHashcode(it.getTarget()), it.getRelClass())).rdd();
            final JavaRDD<ConnectedComponent> cc = GraphProcessor.findCCs(vertexes.rdd(), edgeRdd, dedupConf.getWf().getMaxIterations()).toJavaRDD();
            final Dataset<Relation> mergeRelation = spark.createDataset(cc.filter(k -> k.getDocIds().size() > 1)
                    .flatMap(this::ccToMergeRel).rdd(), Encoders.bean(Relation.class));

            mergeRelation
                    .write().mode("overwrite")
                    .save(DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity));
        }

    }

    public Iterator<Relation> ccToMergeRel(ConnectedComponent cc){
        return cc.getDocIds()
                .stream()
                .flatMap(id -> {
                    List<Relation> tmp = new ArrayList<>();
                    Relation r = new Relation();
                    r.setSource(cc.getCcId());
                    r.setTarget(id);
                    r.setRelClass("merges");
                    tmp.add(r);
                    r = new Relation();
                    r.setTarget(cc.getCcId());
                    r.setSource(id);
                    r.setRelClass("isMergedIn");
                    tmp.add(r);
                    return tmp.stream();
                }).iterator();
    }

    public  static long getHashcode(final String id) {
        return Hashing.murmur3_128().hashString(id).asLong();
    }

}
