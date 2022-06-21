
package eu.dnetlib.dhp.collection.plugin.file;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.utils.XMLIterator;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public abstract class AbstractSplittedRecordPlugin implements CollectorPlugin {

	private static final Logger log = LoggerFactory.getLogger(AbstractSplittedRecordPlugin.class);

	public static final String SPLIT_ON_ELEMENT = "splitOnElement";

	private final FileSystem fileSystem;

	public AbstractSplittedRecordPlugin(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {

		// get path to file
		final Path filePath = Optional
			.ofNullable(api.getBaseUrl())
			.map(Path::new)
			.orElseThrow(() -> new CollectorException("missing baseUrl"));

		log.info("baseUrl: {}", filePath);

		// check that path to file exists
		try {
			if (!fileSystem.exists(filePath)) {
				throw new CollectorException("path does not exist: " + filePath);
			}
		} catch (IOException e) {
			throw new CollectorException(e);
		}

		// get split element
		final String splitOnElement = Optional
			.ofNullable(api.getParams().get(SPLIT_ON_ELEMENT))
			.orElseThrow(
				() -> new CollectorException(String
					.format("missing parameter '%s', required by the AbstractSplittedRecordPlugin", SPLIT_ON_ELEMENT)));

		log.info("splitOnElement: {}", splitOnElement);

		final BufferedInputStream bis = getBufferedInputStream(filePath);

		Iterator<String> xmlIterator = new XMLIterator(splitOnElement, bis);

		return StreamSupport
			.stream(
				Spliterators.spliteratorUnknownSize(xmlIterator, Spliterator.ORDERED),
				false);
	}

	abstract protected BufferedInputStream getBufferedInputStream(final Path filePath) throws CollectorException;

	public FileSystem getFileSystem() {
		return fileSystem;
	}
}
