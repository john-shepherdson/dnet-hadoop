
package eu.dnetlib.dhp.collection.plugin.file;

import java.io.BufferedInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.collection.CollectorException;

public class FileCollectorPlugin extends AbstractSplittedRecordPlugin {

	private static final Logger log = LoggerFactory.getLogger(FileCollectorPlugin.class);

	public FileCollectorPlugin(FileSystem fileSystem) {
		super(fileSystem);
	}

	@Override
	protected BufferedInputStream getBufferedInputStream(final Path filePath) throws CollectorException {

		log.info("filePath: {}", filePath);

		try {
			FileSystem fs = super.getFileSystem();
			return new BufferedInputStream(fs.open(filePath));
		} catch (Exception e) {
			throw new CollectorException("Error reading file " + filePath, e);
		}
	}
}
