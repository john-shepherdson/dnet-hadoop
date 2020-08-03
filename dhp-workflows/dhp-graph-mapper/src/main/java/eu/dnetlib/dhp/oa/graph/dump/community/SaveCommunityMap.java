
package eu.dnetlib.dhp.oa.graph.dump.community;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.QueryInformationSystem;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.graph.CreateContextEntities;
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

//		Boolean isSparkSessionManaged = Optional
//			.ofNullable(parser.get("isSparkSessionManaged"))
//			.map(Boolean::valueOf)
//			.orElse(Boolean.TRUE);
//		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final SaveCommunityMap scm = new SaveCommunityMap(outputPath, nameNode, isLookUpUrl);

		scm.saveCommunityMap();

		// CommunityMap communityMap = queryInformationSystem.getCommunityMap();

//		SparkConf conf = new SparkConf();
//
//		runWithSparkSession(
//			conf,
//			isSparkSessionManaged,
//			spark -> {
//				Utils.removeOutputDir(spark, outputPath);
//
////					execDump(spark, inputPath, outputPath, communityMap, inputClazz, outputClazz, graph);// ,
//				// dumpClazz);
//			});

//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", nameNode);
//		FileSystem fileSystem = FileSystem.get(conf);
//		Path hdfsWritePath = new Path(outputPath);
//		FSDataOutputStream fsDataOutputStream = null;
//		if (fileSystem.exists(hdfsWritePath)) {
//			fsDataOutputStream = fileSystem.append(hdfsWritePath);
//		} else {
//			fsDataOutputStream = fileSystem.create(hdfsWritePath);
//		}
//
//		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
//
//		writer.write(OBJECT_MAPPER.writeValueAsString(communityMap));
//		writer.close();
	}

	private void saveCommunityMap() throws ISLookUpException, IOException {
		writer.write(Utils.OBJECT_MAPPER.writeValueAsString(queryInformationSystem.getCommunityMap()));
		writer.close();
	}
}
