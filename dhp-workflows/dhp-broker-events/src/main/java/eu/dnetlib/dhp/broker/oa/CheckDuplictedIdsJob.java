
package eu.dnetlib.dhp.broker.oa;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import scala.Tuple2;

public class CheckDuplictedIdsJob {

	private static final Logger log = LoggerFactory.getLogger(CheckDuplictedIdsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CheckDuplictedIdsJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/check_duplicates.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String countPath = parser.get("outputDir") + "/counts";
		log.info("countPath: {}", countPath);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final LongAccumulator total = spark.sparkContext().longAccumulator("invaild_event_id");

		final Encoder<Tuple2<String, Long>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.LONG());
		ClusterUtils
			.readPath(spark, eventsPath, Event.class)
			.map((MapFunction<Event, Tuple2<String, Long>>) e -> new Tuple2<>(e.getEventId(), 1l), encoder)
			.groupByKey((MapFunction<Tuple2<String, Long>, String>) t -> t._1, Encoders.STRING())
			.agg(new CountAggregator().toColumn())
			.map((MapFunction<Tuple2<String, Tuple2<String, Long>>, Tuple2<String, Long>>) t -> t._2, encoder)
			.filter((FilterFunction<Tuple2<String, Long>>) t -> t._2 > 1)
			.map(
				(MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>) o -> ClusterUtils
					.incrementAccumulator(o, total),
				encoder)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(countPath);
	}

}

class CountAggregator extends Aggregator<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1395935985734672538L;

	@Override
	public Encoder<Tuple2<String, Long>> bufferEncoder() {
		return Encoders.tuple(Encoders.STRING(), Encoders.LONG());
	}

	@Override
	public Tuple2<String, Long> finish(final Tuple2<String, Long> arg0) {
		return arg0;
	}

	@Override
	public Tuple2<String, Long> merge(final Tuple2<String, Long> arg0, final Tuple2<String, Long> arg1) {
		return doMerge(arg0, arg1);
	}

	@Override
	public Encoder<Tuple2<String, Long>> outputEncoder() {
		return Encoders.tuple(Encoders.STRING(), Encoders.LONG());
	}

	@Override
	public Tuple2<String, Long> reduce(final Tuple2<String, Long> arg0, final Tuple2<String, Long> arg1) {
		return doMerge(arg0, arg1);
	}

	private Tuple2<String, Long> doMerge(final Tuple2<String, Long> arg0, final Tuple2<String, Long> arg1) {
		final String s = StringUtils.defaultIfBlank(arg0._1, arg1._1);
		return new Tuple2<>(s, arg0._2 + arg1._2);
	}

	@Override
	public Tuple2<String, Long> zero() {
		return new Tuple2<>(null, 0l);
	}

}
