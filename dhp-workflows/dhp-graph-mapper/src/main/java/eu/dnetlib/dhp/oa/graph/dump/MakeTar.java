
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.MakeTarArchive;

public class MakeTar implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(MakeTar.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				MakeTar.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_maketar_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final String outputPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", outputPath);

		final String hdfsNameNode = parser.get("nameNode");
		log.info("nameNode: {}", hdfsNameNode);

		final String inputPath = parser.get("sourcePath");
		log.info("input path : {}", inputPath);

		final int gBperSplit = Optional
			.ofNullable(parser.get("splitSize"))
			.map(Integer::valueOf)
			.orElse(10);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		makeTArArchive(fileSystem, inputPath, outputPath, gBperSplit);

	}

	public static void makeTArArchive(FileSystem fileSystem, String inputPath, String outputPath, int gBperSplit)
		throws IOException {

		RemoteIterator<LocatedFileStatus> dirIterator = fileSystem.listLocatedStatus(new Path(inputPath));

		while (dirIterator.hasNext()) {
			LocatedFileStatus fileStatus = dirIterator.next();

			Path p = fileStatus.getPath();
			String pathString = p.toString();
			String entity = pathString.substring(pathString.lastIndexOf("/") + 1);

			MakeTarArchive.tarMaxSize(fileSystem, pathString, outputPath + "/" + entity, entity, gBperSplit);
		}

	}

}
