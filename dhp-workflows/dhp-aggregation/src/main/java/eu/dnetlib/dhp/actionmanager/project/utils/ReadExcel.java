
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
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

/**
 * Applies the parsing of an excel file and writes the Serialization of it in hdfs
 */
public class ReadExcel implements Closeable {
	private static final Log log = LogFactory.getLog(ReadExcel.class);

	private final BufferedWriter writer;
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final InputStream excelFile;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ReadExcel.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/project/parameters.json")));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");
		final String classForName = parser.get("classForName");
		final String sheetName = parser.get("sheetName");

		try (final ReadExcel readExcel = new ReadExcel(hdfsPath, hdfsNameNode, fileURL)) {

			log.info("Getting Excel file...");
			readExcel.execute(classForName, sheetName);

		}
	}

	public void execute(final String classForName, final String sheetName)
		throws IOException, ClassNotFoundException, InvalidFormatException, IllegalAccessException,
		InstantiationException {

		EXCELParser excelParser = new EXCELParser();
		excelParser
			.parse(excelFile, classForName, sheetName)
			.stream()
			.forEach(this::write);
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	public ReadExcel(
		final String hdfsPath,
		final String hdfsNameNode,
		final String fileURL) throws CollectorException, IOException {

		final Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);
		HttpConnector2 httpConnector = new HttpConnector2();
		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsWritePath = new Path(hdfsPath);

		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, false);
		}
		FSDataOutputStream fos = fileSystem.create(hdfsWritePath);

		this.writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
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
