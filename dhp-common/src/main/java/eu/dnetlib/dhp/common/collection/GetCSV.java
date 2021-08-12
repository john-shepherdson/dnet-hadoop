
package eu.dnetlib.dhp.common.collection;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class GetCSV {

	public static void getCsv(FileSystem fileSystem, BufferedReader reader, String hdfsPath,
							  String modelClass) throws IOException, ClassNotFoundException {
		getCsv(fileSystem, reader, hdfsPath, modelClass, ',');

	}

	public static void getCsv(FileSystem fileSystem, BufferedReader reader, String hdfsPath,
		String modelClass, char delimiter) throws IOException, ClassNotFoundException {

		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, false);
		}
		fsDataOutputStream = fileSystem.create(hdfsWritePath);

		try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))){

			ObjectMapper mapper = new ObjectMapper();

			 new CsvToBeanBuilder(reader)
					.withType(Class.forName(modelClass))
					.withSeparator(delimiter)
					.build()
					.parse()
					.forEach(line -> {
						try {
							writer.write(mapper.writeValueAsString(line));
							writer.newLine();
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					});
		}


	}

}
