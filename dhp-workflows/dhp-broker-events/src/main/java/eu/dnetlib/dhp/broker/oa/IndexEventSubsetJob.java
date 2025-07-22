
package eu.dnetlib.dhp.broker.oa;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.BrokerIndexClient;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.index.es.ConvertJSONWithId;

public class IndexEventSubsetJob {

	private static final Logger log = LoggerFactory.getLogger(IndexEventSubsetJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(IndexEventSubsetJob.class
								.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/index_event_subset.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsSubsetPath = parser.get("outputDir") + "/events_subset";
		log.info("eventsSubsetPath: {}", eventsSubsetPath);

		final String index = parser.get("index");
		log.info("index: {}", index);

		final String indexHost = parser.get("esHost");
		log.info("indexHost: {}", indexHost);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final Long date = ClusterUtils
				.readPath(spark, eventsSubsetPath, Event.class)
				.first()
				.getCreationDate();

		try (final BrokerIndexClient feeder = new BrokerIndexClient(indexHost)) {
			final FileSystem fileSystem = FileSystem.get(new Configuration());

			final List<Path> files = ClusterUtils.listFiles(eventsSubsetPath, fileSystem, ".gz");

			log.info("*** Start indexing " + files.size() + " files");
			feeder.parallelBulkIndex(files, 4, fileSystem, new ConvertJSONWithId("\"eventId\":\"((\\d|\\w)*)\"", index));

			log.info("*** Deleting old events");
			feeder.deleteUsingDateBefore(index, "creationDate", date - 1000);

			feeder.refreshIndex(index);
		}

		log.info("*** ALL DONE");

	}

}
