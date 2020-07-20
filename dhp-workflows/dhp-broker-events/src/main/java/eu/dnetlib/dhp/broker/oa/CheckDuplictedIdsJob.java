
package eu.dnetlib.dhp.broker.oa;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("workingPath") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String countPath = parser.get("workingPath") + "/counts";
		log.info("countPath: {}", countPath);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final LongAccumulator total = spark.sparkContext().longAccumulator("invaild_event_id");

		final TypedColumn<Tuple2<String, Long>, Tuple2<String, Long>> agg = new CountAggregator().toColumn();

		ClusterUtils
			.readPath(spark, eventsPath, Event.class)
			.map(e -> new Tuple2<>(e.getEventId(), 1l), Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
			.groupByKey(t -> t._1, Encoders.STRING())
			.agg(agg)
			.map(t -> t._2, Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
			.filter(t -> t._2 > 1)
			.map(o -> ClusterUtils.incrementAccumulator(o, total), Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
			.write()
			.mode(SaveMode.Overwrite)
			.json(countPath);
		;

	}

	private static String eventAsJsonString(final Event f) throws JsonProcessingException {
		return new ObjectMapper().writeValueAsString(f);
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
		final String s = StringUtils.defaultIfBlank(arg0._1, arg1._1);
		return new Tuple2<>(s, arg0._2 + arg1._2);
	}

	@Override
	public Encoder<Tuple2<String, Long>> outputEncoder() {
		return Encoders.tuple(Encoders.STRING(), Encoders.LONG());
	}

	@Override
	public Tuple2<String, Long> reduce(final Tuple2<String, Long> arg0, final Tuple2<String, Long> arg1) {
		final String s = StringUtils.defaultIfBlank(arg0._1, arg1._1);
		return new Tuple2<>(s, arg0._2 + arg1._2);
	}

	@Override
	public Tuple2<String, Long> zero() {
		return new Tuple2<>(null, 0l);
	}

}
