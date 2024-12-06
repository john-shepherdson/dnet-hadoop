
package eu.dnetlib.dhp.collection.orcid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**\
 * The ORCIDExtractor class extracts ORCID data from a TAR archive.
 * The class creates a map of SequenceFile.Writer objects, one for each type of data that is to be extracted (e.g., employments, works, summaries).
 * Then, it iterates over the TAR archive and writes each entry to the appropriate SequenceFile.Writer object.
 * Finally, it closes all the SequenceFile.Writer objects.
 */
public class ORCIDExtractor extends Thread {

	private static final Logger log = LoggerFactory.getLogger(ORCIDExtractor.class);

	private final FileSystem fileSystem;

	private final String id;

	private final Path sourcePath;

	private final String baseOutputPath;

	public ORCIDExtractor(FileSystem fileSystem, String id, Path sourcePath, String baseOutputPath) {
		this.fileSystem = fileSystem;
		this.id = id;
		this.sourcePath = sourcePath;
		this.baseOutputPath = baseOutputPath;
	}

	/**
	 * creates a map of SequenceFile.Writer objects,
	 * one for each type of data that is to be extracted. The map is created based on the filename in the TAR archive.
	 * For example, if the filename is employments.json, the map will contain an entry for the SequenceFile.Writer
	 * object that writes employment data.
	 * @return the Map
	 */
	private Map<String, SequenceFile.Writer> createMap() {
		try {
			log.info("Thread {} Creating sequence files starting from this input Path {}", id, sourcePath.getName());
			Map<String, SequenceFile.Writer> res = new HashMap<>();
			if (sourcePath.getName().contains("summaries")) {

				final String summaryPath = String.format("%s/summaries_%s", baseOutputPath, id);
				final SequenceFile.Writer summary_file = SequenceFile
					.createWriter(
						fileSystem.getConf(),
						SequenceFile.Writer.file(new Path(summaryPath)),
						SequenceFile.Writer.keyClass(Text.class),
						SequenceFile.Writer.valueClass(Text.class));

				log.info("Thread {} Creating only summary path here {}", id, summaryPath);
				res.put("summary", summary_file);
				return res;
			} else {
				String employmentsPath = String.format("%s/employments_%s", baseOutputPath, id);
				final SequenceFile.Writer employments_file = SequenceFile
					.createWriter(
						fileSystem.getConf(),
						SequenceFile.Writer.file(new Path(employmentsPath)),
						SequenceFile.Writer.keyClass(Text.class),
						SequenceFile.Writer.valueClass(Text.class));
				res.put("employments", employments_file);
				log.info("Thread {} Creating employments path here {}", id, employmentsPath);

				final String worksPath = String.format("%s/works_%s", baseOutputPath, id);
				final SequenceFile.Writer works_file = SequenceFile
					.createWriter(
						fileSystem.getConf(),
						SequenceFile.Writer.file(new Path(worksPath)),
						SequenceFile.Writer.keyClass(Text.class),
						SequenceFile.Writer.valueClass(Text.class));
				res.put("works", works_file);
				log.info("Thread {} Creating works path here {}", id, worksPath);

				return res;
			}
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {

		CompressionCodecFactory factory = new CompressionCodecFactory(fileSystem.getConf());
		CompressionCodec codec = factory.getCodec(sourcePath);
		if (codec == null) {
			System.err.println("No codec found for " + sourcePath.getName());
			System.exit(1);
		}

		InputStream gzipInputStream = null;
		try {
			gzipInputStream = codec.createInputStream(fileSystem.open(sourcePath));
			final Map<String, SequenceFile.Writer> fileMap = createMap();
			iterateTar(fileMap, gzipInputStream);

		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			log.info("Closing gzip stream");
			IOUtils.closeStream(gzipInputStream);
		}

	}

	private SequenceFile.Writer retrieveFile(Map<String, SequenceFile.Writer> fileMap, final String path) {
		if (sourcePath.getName().contains("summaries")) {
			return fileMap.get("summary");
		}

		if (path.contains("works")) {
			return fileMap.get("works");
		}
		if (path.contains("employments"))
			return fileMap.get("employments");
		return null;
	}

	private void iterateTar(Map<String, SequenceFile.Writer> fileMap, InputStream gzipInputStream) throws IOException {

		int extractedItem = 0;
		try (final TarArchiveInputStream tais = new TarArchiveInputStream(gzipInputStream)) {

			TarArchiveEntry entry;
			while ((entry = tais.getNextTarEntry()) != null) {

				if (entry.isFile()) {

					final SequenceFile.Writer fl = retrieveFile(fileMap, entry.getName());
					if (fl != null) {
						final Text key = new Text(entry.getName());
						final Text value = new Text(
							org.apache.commons.io.IOUtils.toString(new BufferedReader(new InputStreamReader(tais))));
						fl.append(key, value);
						extractedItem++;
						if (extractedItem % 100000 == 0) {
							log.info("Thread {}: Extracted {} items", id, extractedItem);
						}
					}
				}
			}
		} finally {
			for (SequenceFile.Writer k : fileMap.values()) {
					log.info("Thread {}: Completed processed {} items", id, extractedItem);
				k.hflush();
				k.close();
			}
		}

	}
}
