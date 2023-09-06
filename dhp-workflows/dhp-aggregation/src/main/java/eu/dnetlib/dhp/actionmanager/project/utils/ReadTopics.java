
package eu.dnetlib.dhp.actionmanager.project.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.PrepareProjects;
import eu.dnetlib.dhp.actionmanager.project.utils.model.JsonTopic;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

/**
 * @author miriam.baglioni
 * @Date 28/02/23
 */
public class ReadTopics implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(ReadTopics.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareProjects.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/project/read_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}: ", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode {}", hdfsNameNode);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fs = FileSystem.get(conf);

		readTopics(inputPath, outputPath, fs);
	}

	public static void readTopics(String inputFile, String workingPath, FileSystem fs) throws IOException {
		Path hdfsreadpath = new Path(inputFile);

		FSDataInputStream inputStream = fs.open(hdfsreadpath);

		List<JsonTopic> topics = OBJECT_MAPPER
			.readValue(
				IOUtils.toString(inputStream, "UTF-8"),
				new TypeReference<List<JsonTopic>>() {
				});

		Path hdfsWritePath = new Path(workingPath);

		if (fs.exists(hdfsWritePath)) {
			fs.delete(hdfsWritePath, false);
		}
		FSDataOutputStream fos = fs.create(hdfsWritePath);

		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {

			for (JsonTopic p : topics) {
				writer.write(OBJECT_MAPPER.writeValueAsString(p));
				writer.newLine();
			}
		}
	}
}
