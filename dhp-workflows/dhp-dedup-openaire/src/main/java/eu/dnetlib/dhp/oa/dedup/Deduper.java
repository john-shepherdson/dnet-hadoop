package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.BlockProcessor;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Deduper implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(Deduper.class);

    public static JavaPairRDD<String, String> computeRelations(JavaSparkContext context, JavaPairRDD<String, List<MapDocument>> blocks, DedupConfig config) {
        Map<String, LongAccumulator> accumulators = DedupUtility.constructAccumulator(config, context.sc());

        return blocks.flatMapToPair((PairFlatMapFunction<Tuple2<String, List<MapDocument>>, String, String>) it -> {
            try {
                final SparkReporter reporter = new SparkReporter(accumulators);
                new BlockProcessor(config).processSortedBlock(it._1(), it._2(), reporter);
                return reporter.getRelations().iterator();
            } catch (Exception e) {
                throw new RuntimeException(it._2().get(0).getIdentifier(), e);
            }
        }).mapToPair(
                (PairFunction<Tuple2<String, String>, String, Tuple2<String, String>>) item ->
                        new Tuple2<String, Tuple2<String, String>>(item._1() + item._2(), item))
                .reduceByKey((a, b) -> a)
                .mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>) Tuple2::_2);
    }

    public static JavaPairRDD<String, List<MapDocument>> createSortedBlocks(JavaSparkContext context, JavaPairRDD<String, MapDocument> mapDocs, DedupConfig config) {
        final String of = config.getWf().getOrderField();
        final int maxQueueSize = config.getWf().getGroupMaxSize();
        return mapDocs
                //the reduce is just to be sure that we haven't document with same id
                .reduceByKey((a, b) -> a)
                .map(Tuple2::_2)
                //Clustering: from <id, doc> to List<groupkey,doc>
                .flatMapToPair((PairFlatMapFunction<MapDocument, String, List<MapDocument>>) a ->
                        DedupUtility.getGroupingKeys(config, a)
                                .stream()
                                .map(it -> {
                                            List<MapDocument> tmp = new ArrayList<>();
                                            tmp.add(a);
                                            return new Tuple2<>(it, tmp);
                                        }
                                )
                                .collect(Collectors.toList())
                                .iterator())
                .reduceByKey((Function2<List<MapDocument>, List<MapDocument>, List<MapDocument>>) (v1, v2) -> {
                    v1.addAll(v2);
                    v1.sort(Comparator.comparing(a -> a.getFieldMap().get(of).stringValue()));
                    if (v1.size() > maxQueueSize)
                        return new ArrayList<>(v1.subList(0, maxQueueSize));
                    return v1;
                });
    }
}