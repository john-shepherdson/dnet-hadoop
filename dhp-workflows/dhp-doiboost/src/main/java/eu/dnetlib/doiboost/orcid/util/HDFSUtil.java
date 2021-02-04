
package eu.dnetlib.doiboost.orcid.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {

	public static String readFromTextFile(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		FSDataInputStream inputStream = new FSDataInputStream(fileSystem.open(new Path(path)));
		return IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
	}

	public static void writeToTextFile(String pathValue, String text) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(pathValue);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		FSDataOutputStream os = fileSystem.create(path);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		br.write(text);
		br.close();
		fileSystem.close();
	}
}
