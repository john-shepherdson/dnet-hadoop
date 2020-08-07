
package eu.dnetlib.dhp.oa.graph.dump.graph;

import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.commons.compress.archivers.ar.ArArchiveEntry;
import org.apache.commons.compress.archivers.ar.ArArchiveOutputStream;
import org.apache.commons.crypto.utils.IoUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.APIClient;

public class MakeTar implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(MakeTar.class);
	private final Configuration conf;
	private final ArArchiveOutputStream ar;

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				MakeTar.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_whole/input_maketar_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("nameNode: {}", hdfsNameNode);

		final String inputPath = parser.get("sourcePath");
		log.info("input path : {}", inputPath);

		MakeTar mt = new MakeTar(hdfsPath, hdfsNameNode);
		mt.execute(inputPath);
		mt.close();

	}

	private void execute(String inputPath) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
			.listFiles(
				new Path(inputPath), true);

		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			Path p = fileStatus.getPath();
			String p_string = p.toString();
			if (!p_string.endsWith("_SUCCESS")) {
				String tmp = p_string.substring(0, p_string.lastIndexOf("/"));
				String name = tmp.substring(tmp.lastIndexOf("/") + 1);
				ar.putArchiveEntry(new ArArchiveEntry(name, fileStatus.getLen()));
				InputStream is = fileSystem.open(fileStatus.getPath());
				ar.write(IOUtils.toByteArray(is));
				ar.closeArchiveEntry();
			}

		}
	}

	private void close() throws IOException {
		ar.close();
	}

	public MakeTar(String hdfsPath, String hdfsNameNode) throws IOException {
		this.conf = new Configuration();
		this.conf.set("fs.defaultFS", hdfsNameNode);
		FileSystem fileSystem = FileSystem.get(this.conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fsDataOutputStream = fileSystem.append(hdfsWritePath);
		} else {
			fsDataOutputStream = fileSystem.create(hdfsWritePath);
		}

		this.ar = new ArArchiveOutputStream(fsDataOutputStream.getWrappedStream());

	}

}
