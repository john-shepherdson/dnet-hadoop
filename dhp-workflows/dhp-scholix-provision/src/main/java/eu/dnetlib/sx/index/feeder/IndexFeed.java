
package eu.dnetlib.sx.index.feeder;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.sx.index.RequestManagerFactory;

public class IndexFeed {

	private final FileSystem fileSystem;

	private final static Logger log = LoggerFactory.getLogger(IndexFeed.class);

	public IndexFeed(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							IndexFeed.class
								.getResourceAsStream(
									"/eu/dnetlib/sx/provision/feed_index_params.json"))));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String sourcePath = argumentParser.get("sourcePath");
		log.info("sourcePath is {}", sourcePath);

		final String cluster = argumentParser.get("cluster");
		log.info("cluster is {}", cluster);

		final String index = argumentParser.get("index");
		log.info("index is {}", index);

		final String clusterJson = IOUtils
			.toString(
				Objects
					.requireNonNull(
						IndexFeed.class.getResourceAsStream("/eu/dnetlib/sx/provision/cluster.json")));

		@SuppressWarnings("unchecked")
		final Map<String, String> clusterMap = new ObjectMapper().readValue(clusterJson, Map.class);

		if (!clusterMap.containsKey(cluster)) {
			throw new RuntimeException(
				String.format("Cluster  %s not found, expected values cluster1, cluster2", cluster));
		}

		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));

		new IndexFeed(fileSystem).run(sourcePath, clusterMap.get(cluster), index);
	}

	public void run(final String sourcePath, final String host, final String index) throws Exception {
		RemoteIterator<LocatedFileStatus> ls = fileSystem.listFiles(new Path(sourcePath), false);
		final List<FeedWorker> workers = new ArrayList<>();
		final BlockingQueue<String> queue = new ArrayBlockingQueue<>(3000);
		for (String currentHost : host.split(",")) {
			workers
				.add(
					new FeedWorker(fileSystem, queue, RequestManagerFactory.fromType(index), currentHost.trim(),
						index));
		}
		workers.forEach(Thread::start);
		while (ls.hasNext()) {
			LocatedFileStatus current = ls.next();
			if (current.getPath().getName().endsWith(".gz")) {
				queue.put(current.getPath().toString());
			}
		}

		for (FeedWorker worker : workers) {
			try {
				queue.put(FeedWorker.JOB_COMPLETE);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			worker.join();
		}
	}
}
