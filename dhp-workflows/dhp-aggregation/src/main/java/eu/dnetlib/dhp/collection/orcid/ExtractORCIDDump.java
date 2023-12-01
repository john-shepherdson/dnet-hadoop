
package eu.dnetlib.dhp.collection.orcid;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class ExtractORCIDDump {
	private static final Logger log = LoggerFactory.getLogger(ExtractORCIDDump.class);

	private final FileSystem fileSystem;

	public ExtractORCIDDump(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							DownloadORCIDDumpApplication.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/collection/orcid/extract_orcid_parameter.json"))));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String sourcePath = argumentParser.get("sourcePath");
		log.info("sourcePath is {}", sourcePath);

		final String targetPath = argumentParser.get("targetPath");
		log.info("targetPath is {}", targetPath);

		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));

		new ExtractORCIDDump(fileSystem).run(sourcePath, targetPath);

	}

	public void run(final String sourcePath, final String targetPath) throws IOException, InterruptedException {
		RemoteIterator<LocatedFileStatus> ls = fileSystem.listFiles(new Path(sourcePath), false);
		final List<ORCIDExtractor> workers = new ArrayList<>();
		int i = 0;
		while (ls.hasNext()) {
			LocatedFileStatus current = ls.next();
			if (current.getPath().getName().endsWith("tar.gz")) {
				workers.add(new ORCIDExtractor(fileSystem, "" + i++, current.getPath(), targetPath));
			}
		}
		workers.forEach(Thread::start);
		for (ORCIDExtractor worker : workers) {
			worker.join();
		}
	}
}
