
package eu.dnetlib.sx;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class ScholexplorerFeeder {

	private static final Logger log = LoggerFactory.getLogger(ScholexplorerFeeder.class);
	private final FileSystem fileSystem;

	public ScholexplorerFeeder(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							ScholexplorerFeeder.class
								.getResourceAsStream(
									"/eu/dnetlib/sx/provisionV8/scholexplorer_parameter.json"))));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String sourcePath = argumentParser.get("sourcePath");
		log.info("sourcePath is {}", sourcePath);

		final String index = argumentParser.get("index");
		log.info("index is {}", index);

		final String indexHost = argumentParser.get("indexHost");
		log.info("indexHost is {}", indexHost);

		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));
		SparkSession spark = SparkSession.builder().getOrCreate();
		new ScholexplorerFeeder(fileSystem).run(sourcePath, index, indexHost);

	}

	public void run(final String sourcePath, final String index, final String indexHost)
		throws IOException, InterruptedException {
		RemoteIterator<LocatedFileStatus> ls = fileSystem.listFiles(new Path(sourcePath), false);
		List<Path> files = new java.util.ArrayList<>();
		while (ls.hasNext()) {
			LocatedFileStatus current = ls.next();
			if (current.getPath().getName().endsWith(".gz")) {
				files.add(current.getPath());
			}
		}

		try (ESFeeder feeder = new ESFeeder(indexHost)) {
			long start = System.currentTimeMillis();
			feeder.parallelBulkIndexScholix(files, "scholix", 10, fileSystem);
			long end = System.currentTimeMillis();
			System.out.println("Time Indexing Scholix: " + (end - start) / 1000 + "s");
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

	}

}
