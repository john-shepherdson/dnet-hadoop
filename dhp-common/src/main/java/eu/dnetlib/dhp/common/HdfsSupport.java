
package eu.dnetlib.dhp.common;

import static eu.dnetlib.dhp.common.ThrowingSupport.rethrowAsRuntimeException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** HDFS utility methods. */
public class HdfsSupport {
	private static final Logger logger = LoggerFactory.getLogger(HdfsSupport.class);

	private HdfsSupport() {
	}

	/**
	 * Checks a path (file or dir) exists on HDFS.
	 *
	 * @param path Path to be checked
	 * @param configuration Configuration of hadoop env
	 */
	public static boolean exists(String path, Configuration configuration) {
		logger.info("Checking existence for path: {}", path);
		return rethrowAsRuntimeException(
			() -> {
				Path f = new Path(path);
				FileSystem fileSystem = FileSystem.get(configuration);
				return fileSystem.exists(f);
			});
	}

	/**
	 * Removes a path (file or dir) from HDFS.
	 *
	 * @param path Path to be removed
	 * @param configuration Configuration of hadoop env
	 */
	public static void remove(String path, Configuration configuration) {
		logger.info("Removing path: {}", path);
		rethrowAsRuntimeException(
			() -> {
				Path f = new Path(path);
				FileSystem fileSystem = FileSystem.get(configuration);
				if (fileSystem.exists(f)) {
					fileSystem.delete(f, true);
				}
			});
	}

	/**
	 * Lists hadoop files located below path or alternatively lists subdirs under path.
	 *
	 * @param path Path to be listed for hadoop files
	 * @param configuration Configuration of hadoop env
	 * @return List with string locations of hadoop files
	 */
	public static List<String> listFiles(String path, Configuration configuration) {
		logger.info("Listing files in path: {}", path);
		return rethrowAsRuntimeException(
			() -> Arrays
				.stream(FileSystem.get(configuration).listStatus(new Path(path)))
				.filter(FileStatus::isDirectory)
				.map(x -> x.getPath().toString())
				.collect(Collectors.toList()));
	}
}
