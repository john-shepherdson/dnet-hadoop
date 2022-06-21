
package eu.dnetlib.dhp.collection.plugin.file;

import java.io.BufferedInputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.collection.CollectorException;

public class FileGZipCollectorPlugin extends AbstractSplittedRecordPlugin {

	private static final Logger log = LoggerFactory.getLogger(FileGZipCollectorPlugin.class);

	public FileGZipCollectorPlugin(FileSystem fileSystem) {
		super(fileSystem);
	}

	@Override
	protected BufferedInputStream getBufferedInputStream(final Path filePath) throws CollectorException {

		log.info("filePath: {}", filePath);

		try {
			FileSystem fs = super.getFileSystem();
			GZIPInputStream stream = new GZIPInputStream(fs.open(filePath));
			return new BufferedInputStream(stream);
		} catch (Exception e) {
			throw new CollectorException("Error reading file " + filePath, e);
		}
	}
}
