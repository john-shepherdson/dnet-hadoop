
package eu.dnetlib.dhp.oa.graph.hostedbymap;

import java.io.*;
import java.util.Objects;

import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class DownloadFile {

	private static final Logger log = LoggerFactory.getLogger(DownloadFile.class);

	public static final char DEFAULT_DELIMITER = ',';

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							DownloadFile.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/oa/graph/hostedbymap/download_file_parameters.json"))));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		log.info("fileURL {}", fileURL);

		final String outputFile = parser.get("outputFile");
		log.info("outputFile {}", outputFile);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode {}", hdfsNameNode);

		Path path = new Path(outputFile);
		FileSystem fileSystem = path.getFileSystem(DHPUtils.getHadoopConfiguration(hdfsNameNode));

		new DownloadFile().doDownload(fileURL, outputFile, fileSystem);

	}

	protected void doDownload(String fileURL, String outputFile, FileSystem fileSystem)
		throws IOException, CollectorException {

		try (InputStream reader = new HttpConnector2().getInputSourceAsBinary(fileURL)) {

			Path hdfsWritePath = new Path(outputFile);
			if (fileSystem.exists(hdfsWritePath)) {
				fileSystem.delete(hdfsWritePath, false);
			}

			try (FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath)) {
				byte[] buffer = new byte[64 * 1024]; // Use a larger buffer for efficiency
				int bytesRead;
				while ((bytesRead = reader.read(buffer)) != -1) {
					fsDataOutputStream.write(buffer, 0, bytesRead);
				}
			}
		}
	}
}
