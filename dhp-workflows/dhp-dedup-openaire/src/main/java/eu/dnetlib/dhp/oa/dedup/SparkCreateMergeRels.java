package eu.dnetlib.dhp.oa.dedup;

import com.google.common.hash.Hashing;
import eu.dnetlib.dhp.oa.dedup.graph.ConnectedComponent;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.dedup.graph.GraphProcessor;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkCreateMergeRels extends AbstractSparkAction {

    public static final String PROVENANCE_ACTION_CLASS = "sysimport:dedup";
    private static final Logger log = LoggerFactory.getLogger(SparkCreateMergeRels.class);
    public static final String DNET_PROVENANCE_ACTIONS = "dnet:provenanceActions";

    public SparkCreateMergeRels(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));
        parser.parseArgument(args);

        final String isLookUpUrl = parser.get("isLookUpUrl");
        log.info("isLookupUrl {}", isLookUpUrl);

        new SparkCreateMergeRels(parser, getSparkSession(parser)).run(ISLookupClientFactory.getLookUpService(isLookUpUrl));
    }

    @Override
    public void run(ISLookUpService isLookUpService) throws ISLookUpException, DocumentException, IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");

        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("isLookUpUrl:   '{}'", isLookUpUrl);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        for (DedupConfig dedupConf: getConfigurations(isLookUpService, actionSetId)) {
            final String subEntity = dedupConf.getWf().getSubEntityValue();

            log.info("Creating mergerels for: '{}'", subEntity);

            final int maxIterations = dedupConf.getWf().getMaxIterations();
            log.info("Max iterations {}", maxIterations);

            final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);
            final JavaPairRDD<Object, String> vertexes = sc.textFile(graphBasePath + "/" + subEntity)
                    .map(s -> MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), s))
                    .mapToPair((PairFunction<String, Object, String>)
                            s -> new Tuple2<>(getHashcode(s), s));

            final Dataset<Relation> similarityRelations = spark
                    .read()
                    .load(DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity))
                    .as(Encoders.bean(Relation.class));

            final RDD<Edge<String>> edgeRdd = similarityRelations
                    .javaRDD()
                    .map(it -> new Edge<>(getHashcode(it.getSource()), getHashcode(it.getTarget()), it.getRelClass()))
                    .rdd();

            final RDD<Relation> connectedComponents = GraphProcessor.findCCs(vertexes.rdd(), edgeRdd, maxIterations)
                    .toJavaRDD()
                    .filter(k -> k.getDocIds().size() > 1)
                    .flatMap(cc -> ccToMergeRel(cc, dedupConf))
                    .rdd();

            spark
                    .createDataset(connectedComponents, Encoders.bean(Relation.class))
                    .write()
                    .mode(SaveMode.Append)
                    .save(mergeRelPath);
        }

    }

    public Iterator<Relation> ccToMergeRel(ConnectedComponent cc, DedupConfig dedupConf){
        return cc.getDocIds()
                .stream()
                .flatMap(id -> {
                    List<Relation> tmp = new ArrayList<>();

                    tmp.add(rel(cc.getCcId(), id, "merges", dedupConf));
                    tmp.add(rel(id, cc.getCcId(), "isMergedIn", dedupConf));

                    return tmp.stream();
                }).iterator();
    }

    private Relation rel(String source, String target, String relClass, DedupConfig dedupConf) {
        Relation r = new Relation();
        r.setSource(source);
        r.setTarget(target);
        r.setRelClass(relClass);
        r.setSubRelType("dedup");

        DataInfo info = new DataInfo();
        info.setDeletedbyinference(false);
        info.setInferred(true);
        info.setInvisible(false);
        info.setInferenceprovenance(dedupConf.getWf().getConfigurationId());
        Qualifier provenanceAction = new Qualifier();
        provenanceAction.setClassid(PROVENANCE_ACTION_CLASS);
        provenanceAction.setClassname(PROVENANCE_ACTION_CLASS);
        provenanceAction.setSchemeid(DNET_PROVENANCE_ACTIONS);
        provenanceAction.setSchemename(DNET_PROVENANCE_ACTIONS);
        info.setProvenanceaction(provenanceAction);

        //TODO calculate the trust value based on the similarity score of the elements in the CC
        //info.setTrust();

        r.setDataInfo(info);
        return r;
    }

    public  static long getHashcode(final String id) {
        return Hashing.murmur3_128().hashString(id).asLong();
    }

}
