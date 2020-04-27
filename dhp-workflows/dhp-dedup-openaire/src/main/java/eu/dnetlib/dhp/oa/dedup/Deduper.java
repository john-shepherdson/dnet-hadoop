
package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.oa.dedup.model.Block;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.BlockProcessor;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Serializable;
import scala.Tuple2;

public class Deduper implements Serializable {

	public static JavaPairRDD<String, String> computeRelations(
		JavaSparkContext context, JavaPairRDD<String, Block> blocks, DedupConfig config) {
		Map<String, LongAccumulator> accumulators = DedupUtility.constructAccumulator(config, context.sc());

		return blocks
			.flatMapToPair(
				it -> {
					final SparkReporter reporter = new SparkReporter(accumulators);
					new BlockProcessor(config)
						.processSortedBlock(it._1(), it._2().getDocuments(), reporter);
					return reporter.getRelations().iterator();
				})
			.mapToPair(it -> new Tuple2<>(it._1() + it._2(), it))
			.reduceByKey((a, b) -> a)
			.mapToPair(Tuple2::_2);
	}

	public static JavaPairRDD<String, Block> createSortedBlocks(
		JavaPairRDD<String, MapDocument> mapDocs, DedupConfig config) {
		final String of = config.getWf().getOrderField();
		final int maxQueueSize = config.getWf().getGroupMaxSize();

		return mapDocs
			// the reduce is just to be sure that we haven't document with same id
			.reduceByKey((a, b) -> a)
			.map(Tuple2::_2)
			// Clustering: from <id, doc> to List<groupkey,doc>
			.flatMap(
				a -> DedupUtility
					.getGroupingKeys(config, a)
					.stream()
					.map(it -> Block.from(it, a))
					.collect(Collectors.toList())
					.iterator())
			.mapToPair(block -> new Tuple2<>(block.getKey(), block))
			.reduceByKey((b1, b2) -> Block.from(b1, b2, of, maxQueueSize));
	}
}
