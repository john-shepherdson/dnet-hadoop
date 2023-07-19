
package eu.dnetlib.dhp.common;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class MakeTarArchive implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(MakeTarArchive.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				MakeTarArchive.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/common/input_maketar_parameters.json"));

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

	private static TarArchiveOutputStream getTar(FileSystem fileSystem, String outputPath) throws IOException {
		Path hdfsWritePath = new Path(outputPath);
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, true);

		}
		return new TarArchiveOutputStream(fileSystem.create(hdfsWritePath).getWrappedStream());
	}

	private static void write(FileSystem fileSystem, String inputPath, String outputPath, String dirName)
		throws IOException {

		Path hdfsWritePath = new Path(outputPath);
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, true);

		}
		try (TarArchiveOutputStream ar = new TarArchiveOutputStream(
			fileSystem.create(hdfsWritePath).getWrappedStream())) {

			RemoteIterator<LocatedFileStatus> iterator = fileSystem
				.listFiles(
					new Path(inputPath), true);

			while (iterator.hasNext()) {
				writeCurrentFile(fileSystem, dirName, iterator, ar, 0);
			}

		}
	}

	public static void tarMaxSize(FileSystem fileSystem, String inputPath, String outputPath, String dir_name,
		int gBperSplit) throws IOException {
		final long bytesPerSplit = 1024L * 1024L * 1024L * gBperSplit;

		long sourceSize = fileSystem.getContentSummary(new Path(inputPath)).getSpaceConsumed();

		if (sourceSize < bytesPerSplit) {
			write(fileSystem, inputPath, outputPath + ".tar", dir_name);
		} else {
			int partNum = 0;

			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem
				.listFiles(
					new Path(inputPath), true);
			boolean next = fileStatusListIterator.hasNext();
			while (next) {
				try (TarArchiveOutputStream ar = getTar(fileSystem, outputPath + "_" + (partNum + 1) + ".tar")) {

					long currentSize = 0;
					while (next && currentSize < bytesPerSplit) {
						currentSize = writeCurrentFile(fileSystem, dir_name, fileStatusListIterator, ar, currentSize);
						next = fileStatusListIterator.hasNext();

					}

					partNum += 1;
				}
			}
		}
	}

	private static long writeCurrentFile(FileSystem fileSystem, String dirName,
		RemoteIterator<LocatedFileStatus> fileStatusListIterator,
		TarArchiveOutputStream ar, long currentSize) throws IOException {
		LocatedFileStatus fileStatus = fileStatusListIterator.next();

		Path p = fileStatus.getPath();
		String pString = p.toString();
		if (!pString.endsWith("_SUCCESS")) {
			String name = pString.substring(pString.lastIndexOf("/") + 1);
			if (name.startsWith("part-") & name.length() > 10) {
				String tmp = name.substring(0, 10);
				if (name.contains(".")) {
					tmp += name.substring(name.indexOf("."));
				}
				name = tmp;
			}
			TarArchiveEntry entry = new TarArchiveEntry(dirName + "/" + name);
			entry.setSize(fileStatus.getLen());
			currentSize += fileStatus.getLen();
			ar.putArchiveEntry(entry);

			InputStream is = fileSystem.open(fileStatus.getPath());

			BufferedInputStream bis = new BufferedInputStream(is);

			int count;
			byte[] data = new byte[1024];
			while ((count = bis.read(data, 0, data.length)) != -1) {
				ar.write(data, 0, count);
			}
			bis.close();
			ar.closeArchiveEntry();

		}
		return currentSize;
	}

}
