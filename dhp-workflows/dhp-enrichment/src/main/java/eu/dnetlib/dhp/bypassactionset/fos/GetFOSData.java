
package eu.dnetlib.dhp.bypassactionset.fos;

import java.io.*;
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

	public static final char DEFAULT_DELIMITER = '\t';

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							GetFOSData.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/bypassactionset/get_fos_parameters.json"))));

		parser.parseArgument(args);

		// the path where the original fos csv file is stored
		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}", inputPath);

		// the path where to put the file as json
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

		new GetFOSData().doRewrite(inputPath, outputFile, classForName, delimiter, fileSystem);

	}

	public void doRewrite(String inputPath, String outputFile, String classForName, char delimiter, FileSystem fs)
		throws IOException, ClassNotFoundException {

		// reads the csv and writes it as its json equivalent
		try (InputStreamReader reader = new InputStreamReader(fs.open(new Path(inputPath)))) {
			GetCSV.getCsv(fs, reader, outputFile, classForName, delimiter);
		}

	}

}
