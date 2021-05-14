
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.*;
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
 * Applies the parsing of an excel file and writes the Serialization of it in hdfs
 */
public class ReadExcel implements Closeable {
	private static final Log log = LogFactory.getLog(ReadCSV.class);
	private final Configuration conf;
	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final InputStream excelFile;

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

		try (final ReadExcel readExcel = new ReadExcel(hdfsPath, hdfsNameNode, fileURL)) {

			log.info("Getting Excel file...");
			readExcel.execute(classForName);

		}
	}

	public void execute(final String classForName) throws Exception {
		EXCELParser excelParser = new EXCELParser();
		excelParser
			.parse(excelFile, classForName)
			.stream()
			.forEach(p -> write(p));

	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	public ReadExcel(
		final String hdfsPath,
		final String hdfsNameNode,
		final String fileURL)
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
		this.excelFile = httpConnector.getInputSourceAsStream(fileURL);
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
