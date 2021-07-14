
package eu.dnetlib.doiboost.orcid.util;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.doiboost.orcid.SparkDownloadOrcidAuthors;

public class HDFSUtil {

	static Logger logger = LoggerFactory.getLogger(HDFSUtil.class);

	private static FileSystem getFileSystem(String hdfsServerUri) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsServerUri);
		FileSystem fileSystem = FileSystem.get(conf);
		return fileSystem;
	}

	public static String readFromTextFile(String hdfsServerUri, String workingPath, String path) throws IOException {
		FileSystem fileSystem = getFileSystem(hdfsServerUri);
		Path toReadPath = new Path(workingPath.concat(path));
		if (!fileSystem.exists(toReadPath)) {
			throw new RuntimeException("File not exist: " + path);
		}
		logger.info("Last_update_path " + toReadPath);
		FSDataInputStream inputStream = new FSDataInputStream(fileSystem.open(toReadPath));
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		StringBuffer sb = new StringBuffer();
		try {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} finally {
			br.close();
		}
		String buffer = sb.toString();
		logger.info("Last_update: " + buffer);
		return buffer;
	}

	public static void writeToTextFile(String hdfsServerUri, String workingPath, String path, String text)
		throws IOException {
		FileSystem fileSystem = getFileSystem(hdfsServerUri);
		Path toWritePath = new Path(workingPath.concat(path));
		if (fileSystem.exists(toWritePath)) {
			fileSystem.delete(toWritePath, true);
		}
		FSDataOutputStream os = fileSystem.create(toWritePath);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
		br.write(text);
		br.close();
	}
}
