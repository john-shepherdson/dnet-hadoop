
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.HttpConnector2;

/**
 * Applies the parsing of a csv file and writes the Serialization of it in hdfs
 */
public class ReadCSV implements Closeable {
	private static final Log log = LogFactory.getLog(ReadCSV.class);
	private final Configuration conf;
	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final String csvFile;
	private final char delimiter;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ReadCSV.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/project/parameters.json")));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");
		final String classForName = parser.get("classForName");
		Optional<String> delimiter = Optional.ofNullable(parser.get("delimiter"));
		char del = ';';
		if (delimiter.isPresent())
			del = delimiter.get().charAt(0);
		try (final ReadCSV readCSV = new ReadCSV(hdfsPath, hdfsNameNode, fileURL, del)) {

			log.info("Getting CSV file...");
			readCSV.execute(classForName);

		}

	}

	public void execute(final String classForName) throws Exception {
		CSVParser csvParser = new CSVParser();
		csvParser
			.parse(csvFile, classForName, delimiter)
			.stream()
			.forEach(p -> write(p));

	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	public ReadCSV(
		final String hdfsPath,
		final String hdfsNameNode,
		final String fileURL,
		char delimiter)
		throws Exception {
		this.conf = new Configuration();
		this.conf.set("fs.defaultFS", hdfsNameNode);
		HttpConnector2 httpConnector = new HttpConnector2();
		FileSystem fileSystem = FileSystem.get(this.conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, false);
		}
		fsDataOutputStream = fileSystem.create(hdfsWritePath);

		this.writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
		this.csvFile = httpConnector.getInputSource(fileURL);
		this.delimiter = delimiter;
	}

	protected void write(final Object p) {
		try {
			writer.write(OBJECT_MAPPER.writeValueAsString(p));
			writer.newLine();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

}
