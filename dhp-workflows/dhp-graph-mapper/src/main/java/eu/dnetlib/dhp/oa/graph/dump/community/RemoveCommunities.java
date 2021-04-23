
package eu.dnetlib.dhp.oa.graph.dump.community;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class RemoveCommunities implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(RemoveCommunities.class);
	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final Configuration conf;
	private final BufferedWriter writer;
	private final CommunityMap communityMap;

	public RemoveCommunities(String path, String hdfsNameNode) throws IOException {
		conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);
		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsPath = new Path(path);
		// FSDataInputStream p = fileSystem.open(hdfsPath);
		// ObjectMapper mapper = new ObjectMapper();
		communityMap = OBJECT_MAPPER.readValue((InputStream) fileSystem.open(hdfsPath), CommunityMap.class);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsPath)) {
			fileSystem.delete(hdfsPath);
		}
		fsDataOutputStream = fileSystem.create(hdfsPath);

		writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

	}

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				RemoveCommunities.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_rc_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final String nameNode = parser.get("nameNode");
		log.info("nameNode: {}", nameNode);

		final String outputPath = parser.get("path");
		log.info("outputPath: {}", outputPath);

		final String communityId = parser.get("communityId");

		final RemoveCommunities scm = new RemoveCommunities(outputPath, nameNode);

		scm.removeCommunities(communityId);

	}

	private void removeCommunities(String communityId) throws IOException {
		Set<String> toRemove = communityMap.keySet().stream().map(key -> {
			if (key.equals(communityId))
				return null;
			return key;
		}).filter(Objects::nonNull).collect(Collectors.toSet());

		toRemove.forEach(key -> communityMap.remove(key));
		writer.write(OBJECT_MAPPER.writeValueAsString(communityMap));
		writer.close();
	}

}
