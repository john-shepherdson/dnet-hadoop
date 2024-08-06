
package eu.dnetlib.dhp.collection.datacite;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class DumpExtractor {

	private static final Logger log = LoggerFactory.getLogger(DumpExtractor.class);

	public static InputStream createInputStream(FileSystem fileSystem, Path sourcePath) throws IOException {
		CompressionCodecFactory factory = new CompressionCodecFactory(fileSystem.getConf());
		CompressionCodec codec = factory.getCodec(sourcePath);
		if (codec == null) {
			System.err.println("No codec found for " + sourcePath.getName());
			System.exit(1);
		}

		return codec.createInputStream(fileSystem.open(sourcePath));
	}

	public static void iterateTar(SequenceFile.Writer sfile, InputStream gzipInputStream) throws IOException {

		int extractedItem = 0;
		try (final TarArchiveInputStream tais = new TarArchiveInputStream(gzipInputStream)) {

			TarArchiveEntry entry;
			while ((entry = tais.getNextTarEntry()) != null) {
				if (entry.isFile()) {
					if (sfile != null) {
						final Text key = new Text(entry.getName());
						final BufferedReader br = new BufferedReader(new InputStreamReader(tais));
						while (br.ready()) {
							sfile.append(key, new Text(br.readLine()));
							extractedItem++;
						}
						if (extractedItem % 100000 == 0) {
							log.info("Extracted {} items", extractedItem);
						}
					}
				}
			}
		} finally {
			if (sfile != null) {
				sfile.hflush();
				sfile.close();
			}
		}
	}

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							DumpExtractor.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/datacite/extract_datacite_parameter.json"))));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String sourcePath = argumentParser.get("sourcePath");
		log.info("sourcePath is {}", sourcePath);

		final String targetPath = argumentParser.get("targetPath");
		log.info("targetPath is {}", targetPath);

		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));

		final Path sPath = new Path(sourcePath);

		final InputStream gzipInputStream = createInputStream(fileSystem, sPath);

		final SequenceFile.Writer outputFile = SequenceFile
			.createWriter(
				fileSystem.getConf(),
				SequenceFile.Writer.file(new Path(targetPath)),
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class));

		iterateTar(outputFile, gzipInputStream);
		gzipInputStream.close();

	}

}
