
package eu.dnetlib.dhp.oa.graph.hostedbymap;

import java.io.*;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.GetCSV;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class DownloadCSV {

	private static final Logger log = LoggerFactory.getLogger(DownloadCSV.class);

	public static final char DEFAULT_DELIMITER = ';';

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

		final String workingPath = parser.get("workingPath");
		log.info("workingPath {}", workingPath);

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

		final HttpConnector2 connector2 = new HttpConnector2();

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);
		final Path path = new Path(workingPath + "/replaced.csv");

		try (BufferedReader in = new BufferedReader(
			new InputStreamReader(connector2.getInputSourceAsStream(fileURL)))) {

			try (FSDataOutputStream fos = fileSystem.create(path, true)) {
				String line;
				while ((line = in.readLine()) != null) {
					fos.writeUTF(line.replace("\\\"", "\""));
					fos.writeUTF("\n");
				}
			}
		}

		try (InputStreamReader reader = new InputStreamReader(fileSystem.open(path))) {
			GetCSV.getCsv(fileSystem, reader, outputFile, classForName, delimiter);
		}

	}

}
