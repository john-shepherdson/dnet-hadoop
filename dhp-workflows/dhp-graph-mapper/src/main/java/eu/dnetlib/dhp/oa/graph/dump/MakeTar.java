
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;

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

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);

		makeTArArchive(fileSystem, inputPath, outputPath);

	}

	public static void makeTArArchive(FileSystem fileSystem, String inputPath, String outputPath) throws IOException {

		RemoteIterator<LocatedFileStatus> dir_iterator = fileSystem.listLocatedStatus(new Path(inputPath));

		while (dir_iterator.hasNext()) {
			LocatedFileStatus fileStatus = dir_iterator.next();

			Path p = fileStatus.getPath();
			String p_string = p.toString();
			String entity = p_string.substring(p_string.lastIndexOf("/") + 1);

			writeMaxSize(fileSystem, p_string, outputPath + "/" + entity, entity, 40);
		}

	}

	private static TarArchiveOutputStream getTar(FileSystem fileSystem, String outputPath) throws IOException {
		Path hdfsWritePath = new Path(outputPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, true);

		}
		fsDataOutputStream = fileSystem.create(hdfsWritePath);

		return new TarArchiveOutputStream(fsDataOutputStream.getWrappedStream());
	}

	private static void writeMaxSize(FileSystem fileSystem, String inputPath, String outputPath, String dir_name,
		int gBperSplit) throws IOException {
		final long bytesPerSplit = 1024L * 1024L * 1024L * gBperSplit;

		long sourceSize = fileSystem.getContentSummary(new Path(inputPath)).getSpaceConsumed();

		final long numberOfSplits = sourceSize / bytesPerSplit;

		if (numberOfSplits <= 1) {
			write(fileSystem, inputPath, outputPath + ".tar", dir_name);
		} else {
			int partNum = 0;
			long remainingBytes = sourceSize % bytesPerSplit;

			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
				.listFiles(
					new Path(inputPath), true);
			while (sourceSize > 0) {
				TarArchiveOutputStream ar = getTar(fileSystem, outputPath + "_" + partNum + ".tar");

				long current_size = 0;
				while (fileStatusListIterator.hasNext() && current_size < bytesPerSplit) {
					LocatedFileStatus fileStatus = fileStatusListIterator.next();

					Path p = fileStatus.getPath();
					String p_string = p.toString();
					if (!p_string.endsWith("_SUCCESS")) {
						String name = p_string.substring(p_string.lastIndexOf("/") + 1);
						if (name.trim().equalsIgnoreCase("communities_infrastructures")) {
							name = "communities_infrastructures.json";
						}
						TarArchiveEntry entry = new TarArchiveEntry(dir_name + "/" + name);
						entry.setSize(fileStatus.getLen());
						current_size += fileStatus.getLen();
						ar.putArchiveEntry(entry);

						InputStream is = fileSystem.open(fileStatus.getPath());

						BufferedInputStream bis = new BufferedInputStream(is);

						int count;
						byte data[] = new byte[1024];
						while ((count = bis.read(data, 0, data.length)) != -1) {
							ar.write(data, 0, count);
						}
						bis.close();
						ar.closeArchiveEntry();

					}

				}
				sourceSize = sourceSize - current_size;
				partNum += 1;
				ar.close();
			}

		}

	}

	private static void write(FileSystem fileSystem, String inputPath, String outputPath, String dir_name)
		throws IOException {

		Path hdfsWritePath = new Path(outputPath);
		FSDataOutputStream fsDataOutputStream = null;
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, true);

		}
		fsDataOutputStream = fileSystem.create(hdfsWritePath);

		TarArchiveOutputStream ar = new TarArchiveOutputStream(fsDataOutputStream.getWrappedStream());

		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
			.listFiles(
				new Path(inputPath), true);

		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			Path p = fileStatus.getPath();
			String p_string = p.toString();
			if (!p_string.endsWith("_SUCCESS")) {
				String name = p_string.substring(p_string.lastIndexOf("/") + 1);
				if (name.trim().equalsIgnoreCase("communities_infrastructures")) {
					name = "communities_infrastructures.json";
				}
				TarArchiveEntry entry = new TarArchiveEntry(dir_name + "/" + name);
				entry.setSize(fileStatus.getLen());
				ar.putArchiveEntry(entry);

				InputStream is = fileSystem.open(fileStatus.getPath());

				BufferedInputStream bis = new BufferedInputStream(is);

				int count;
				byte data[] = new byte[1024];
				while ((count = bis.read(data, 0, data.length)) != -1) {
					ar.write(data, 0, count);
				}
				bis.close();
				ar.closeArchiveEntry();

			}

		}

		ar.close();
	}

}
