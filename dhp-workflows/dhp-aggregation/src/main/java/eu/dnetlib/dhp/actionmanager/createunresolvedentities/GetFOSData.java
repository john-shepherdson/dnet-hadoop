
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.GetCSV;

public class GetFOSData implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(GetFOSData.class);

	public static final char DEFAULT_DELIMITER = ',';

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							GetFOSData.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/actionmanager/createunresolvedentities/get_fos_parameters.json"))));

		parser.parseArgument(args);

		// the path where the original fos csv file is stored
		final String sourcePath = parser.get("sourcePath");
		log.info("sourcePath {}", sourcePath);

		// the path where to put the file as json
		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

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

		new GetFOSData().doRewrite(sourcePath, outputPath, classForName, delimiter, fileSystem);

	}

	public void doRewrite(String inputPath, String outputFile, String classForName, char delimiter, FileSystem fs)
		throws IOException, ClassNotFoundException {

		// reads the csv and writes it as its json equivalent
		try (InputStreamReader reader = new InputStreamReader(fs.open(new Path(inputPath)))) {
			GetCSV.getCsv(fs, reader, outputFile, classForName, delimiter);
		}

	}

}
