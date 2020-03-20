package eu.dnetlib.dhp.dedup;

import com.google.common.hash.Hashing;
import eu.dnetlib.dhp.dedup.graph.ConnectedComponent;
import eu.dnetlib.dhp.dedup.graph.GraphProcessor;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkCreateConnectedComponent {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateConnectedComponent.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/createCC_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateConnectedComponent().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws ISLookUpException, DocumentException {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");

        try (SparkSession spark = getSparkSession(parser)) {

            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            for (DedupConfig dedupConf: DedupUtility.getConfigurations(isLookUpUrl, actionSetId)) {

                final String entity = dedupConf.getWf().getEntityType();
                final String subEntity = dedupConf.getWf().getSubEntityValue();

                final JavaPairRDD<Object, String> vertexes = sc.textFile(graphBasePath + "/" + subEntity)
                        .map(s -> MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), s))
                        .mapToPair((PairFunction<String, Object, String>)
                                s -> new Tuple2<Object, String>(getHashcode(s), s)
                        );

                final Dataset<Relation> similarityRelations = spark.read().load(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity)).as(Encoders.bean(Relation.class));
                final RDD<Edge<String>> edgeRdd = similarityRelations.javaRDD().map(it -> new Edge<>(getHashcode(it.getSource()), getHashcode(it.getTarget()), it.getRelClass())).rdd();
                final JavaRDD<ConnectedComponent> cc = GraphProcessor.findCCs(vertexes.rdd(), edgeRdd, dedupConf.getWf().getMaxIterations()).toJavaRDD();
                final Dataset<Relation> mergeRelation = spark.createDataset(cc.filter(k -> k.getDocIds().size() > 1).flatMap((FlatMapFunction<ConnectedComponent, Relation>) c ->
                        c.getDocIds()
                                .stream()
                                .flatMap(id -> {
                                    List<Relation> tmp = new ArrayList<>();
                                    Relation r = new Relation();
                                    r.setSource(c.getCcId());
                                    r.setTarget(id);
                                    r.setRelClass("merges");
                                    tmp.add(r);
                                    r = new Relation();
                                    r.setTarget(c.getCcId());
                                    r.setSource(id);
                                    r.setRelClass("isMergedIn");
                                    tmp.add(r);
                                    return tmp.stream();
                                }).iterator()).rdd(), Encoders.bean(Relation.class));
                mergeRelation.write().mode("overwrite").save(DedupUtility.createMergeRelPath(workingPath, actionSetId, entity));
            }
        }
    }

    public  static long getHashcode(final String id) {
        return Hashing.murmur3_128().hashString(id).asLong();
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }
}
