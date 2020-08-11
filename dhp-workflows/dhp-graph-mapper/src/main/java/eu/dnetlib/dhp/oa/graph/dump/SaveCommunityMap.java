/**
 * This class connects with the IS related to the isLookUpUrl got as parameter.
 * It saves the information about the context that will guide the dump of the results.
 * The information saved is a HashMap. The key is the id of a community - research infrastructure/initiative , the
 * value is the label of the research community - research infrastructure/initiative.
 *
 */

package eu.dnetlib.dhp.oa.graph.dump;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

public class SaveCommunityMap implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SaveCommunityMap.class);
	private final QueryInformationSystem queryInformationSystem;

	private final Configuration conf;
	private final BufferedWriter writer;

	public SaveCommunityMap(String hdfsPath, String hdfsNameNode, String isLookUpUrl) throws IOException {
		conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);
		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fsDataOutputStream = fileSystem.append(hdfsWritePath);
		} else {
			fsDataOutputStream = fileSystem.create(hdfsWritePath);
		}

		queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setIsLookUp(Utils.getIsLookUpService(isLookUpUrl));

		writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

	}

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SaveCommunityMap.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_cm_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final String nameNode = parser.get("nameNode");
		log.info("nameNode: {}", nameNode);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookUpUrl: {}", isLookUpUrl);

		final SaveCommunityMap scm = new SaveCommunityMap(outputPath, nameNode, isLookUpUrl);

		scm.saveCommunityMap();

	}

	private void saveCommunityMap() throws ISLookUpException, IOException {
		writer.write(Utils.OBJECT_MAPPER.writeValueAsString(queryInformationSystem.getCommunityMap()));
		writer.close();
	}
}
