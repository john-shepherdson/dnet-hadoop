
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

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

	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final String csvFile;

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

		try (final ReadCSV readCSV = new ReadCSV(hdfsPath, hdfsNameNode, fileURL)) {

			log.info("Getting CSV file...");
			readCSV.execute(classForName);
		}
	}

	public void execute(final String classForName)
		throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		CSVParser csvParser = new CSVParser();
		csvParser
			.parse(csvFile, classForName)
			.stream()
			.forEach(this::write);
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	public ReadCSV(
		final String hdfsPath,
		final String hdfsNameNode,
		final String fileURL)
		throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);
		HttpConnector2 httpConnector = new HttpConnector2();
		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsWritePath = new Path(hdfsPath);

		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, false);
		}
		final FSDataOutputStream fos = fileSystem.create(hdfsWritePath);

		this.writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
		this.csvFile = httpConnector.getInputSource(fileURL);
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
