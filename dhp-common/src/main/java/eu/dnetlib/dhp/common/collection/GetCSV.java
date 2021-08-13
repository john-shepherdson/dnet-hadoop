
package eu.dnetlib.dhp.common.collection;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;

public class GetCSV {

	public static final char DEFAULT_DELIMITER = ',';

	private GetCSV() {
	}

	public static void getCsv(FileSystem fileSystem, BufferedReader reader, String hdfsPath,
		String modelClass) throws IOException, ClassNotFoundException {
		getCsv(fileSystem, reader, hdfsPath, modelClass, DEFAULT_DELIMITER);
	}

	public static void getCsv(FileSystem fileSystem, Reader reader, String hdfsPath,
		String modelClass, char delimiter) throws IOException, ClassNotFoundException {

		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, false);
		}
		fsDataOutputStream = fileSystem.create(hdfsWritePath);

		try (BufferedWriter writer = new BufferedWriter(
			new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))) {

			final ObjectMapper mapper = new ObjectMapper();

			@SuppressWarnings("unchecked")
			final List lines = new CsvToBeanBuilder(reader)
				.withType(Class.forName(modelClass))
				.withSeparator(delimiter)
				.build()
				.parse();

			for (Object line : lines) {
				writer.write(mapper.writeValueAsString(line));
				writer.newLine();
			}
		}
	}

}
