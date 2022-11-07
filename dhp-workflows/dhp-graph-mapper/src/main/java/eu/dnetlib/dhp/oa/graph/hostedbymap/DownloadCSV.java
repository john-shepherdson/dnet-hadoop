
package eu.dnetlib.dhp.oa.graph.hostedbymap;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.GetCSV;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class DownloadCSV {

	private static final Logger log = LoggerFactory.getLogger(DownloadCSV.class);

	public static final char DEFAULT_DELIMITER = ',';

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							DownloadCSV.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/oa/graph/hostedbymap/download_csv_parameters.json"))));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		log.info("fileURL {}", fileURL);

		final String outputFile = parser.get("outputFile");
		log.info("outputFile {}", outputFile);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode {}", hdfsNameNode);

		final String classForName = parser.get("classForName");
		log.info("classForName {}", classForName);

		final char delimiter = Optional
			.ofNullable(parser.get("delimiter"))
			.map(s -> s.charAt(0))
			.orElse(DEFAULT_DELIMITER);
		log.info("delimiter {}", delimiter);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		new DownloadCSV().doDownload(fileURL, outputFile, classForName, delimiter, fileSystem);

	}

	protected void doDownload(String fileURL, String outputFile, String classForName,
		char delimiter, FileSystem fs)
		throws IOException, ClassNotFoundException, CollectorException {

		try (InputStreamReader reader = new InputStreamReader(new HttpConnector2().getInputSourceAsStream(fileURL))) {
			GetCSV.getCsv(fs, reader, outputFile, classForName, delimiter);
		}
	}

}
