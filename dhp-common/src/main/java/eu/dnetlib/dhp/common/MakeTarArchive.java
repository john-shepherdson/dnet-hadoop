
package eu.dnetlib.dhp.common;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.*;

public class MakeTarArchive implements Serializable {

	private static TarArchiveOutputStream getTar(FileSystem fileSystem, String outputPath) throws IOException {
		Path hdfsWritePath = new Path(outputPath);
		if (fileSystem.exists(hdfsWritePath)) {
			fileSystem.delete(hdfsWritePath, true);

		}
		return new TarArchiveOutputStream(fileSystem.create(hdfsWritePath).getWrappedStream());
	}

	private static void write(FileSystem fileSystem, String inputPath, String outputPath, String dir_name)
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
				writeCurrentFile(fileSystem, dir_name, iterator, ar, 0);
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
				TarArchiveOutputStream ar = getTar(fileSystem, outputPath + "_" + (partNum + 1) + ".tar");

				long current_size = 0;
				while (next && current_size < bytesPerSplit) {
					current_size = writeCurrentFile(fileSystem, dir_name, fileStatusListIterator, ar, current_size);
					next = fileStatusListIterator.hasNext();

				}

				partNum += 1;
				ar.close();
			}

		}

	}

	private static long writeCurrentFile(FileSystem fileSystem, String dir_name,
		RemoteIterator<LocatedFileStatus> fileStatusListIterator,
		TarArchiveOutputStream ar, long current_size) throws IOException {
		LocatedFileStatus fileStatus = fileStatusListIterator.next();

		Path p = fileStatus.getPath();
		String p_string = p.toString();
		if (!p_string.endsWith("_SUCCESS")) {
			String name = p_string.substring(p_string.lastIndexOf("/") + 1);
			TarArchiveEntry entry = new TarArchiveEntry(dir_name + "/" + name);
			entry.setSize(fileStatus.getLen());
			current_size += fileStatus.getLen();
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
		return current_size;
	}

}
