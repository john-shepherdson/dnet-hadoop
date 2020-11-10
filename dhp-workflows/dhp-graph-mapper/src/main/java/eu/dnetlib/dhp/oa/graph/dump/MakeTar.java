
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;
import java.util.Optional;

import org.apache.commons.compress.archivers.ar.ArArchiveEntry;
import org.apache.commons.compress.archivers.ar.ArArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.MakeTarArchive;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;

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

		RemoteIterator<LocatedFileStatus> dir_iterator = fileSystem.listLocatedStatus(new Path(inputPath));

		while (dir_iterator.hasNext()) {
			LocatedFileStatus fileStatus = dir_iterator.next();

			Path p = fileStatus.getPath();
			String p_string = p.toString();
			String entity = p_string.substring(p_string.lastIndexOf("/") + 1);

			MakeTarArchive.tarMaxSize(fileSystem, p_string, outputPath + "/" + entity, entity, gBperSplit);
		}

	}

}
