package eu.dnetlib.dhp.collection.plugin.base;

import java.io.InputStream;
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
import eu.dnetlib.dhp.collection.plugin.file.AbstractSplittedRecordPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class BaseCollectorPlugin implements CollectorPlugin {

	private final FileSystem fs;

	private static final Logger log = LoggerFactory.getLogger(AbstractSplittedRecordPlugin.class);

	public BaseCollectorPlugin(final FileSystem fs) {
		this.fs = fs;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {
		// get path to file
		final Path filePath = Optional
				.ofNullable(api.getBaseUrl())
				.map(Path::new)
				.orElseThrow(() -> new CollectorException("missing baseUrl"));

		log.info("baseUrl: {}", filePath);

		try {
			if (!this.fs.exists(filePath)) { throw new CollectorException("path does not exist: " + filePath); }
		} catch (final Throwable e) {
			throw new CollectorException(e);
		}

		try (InputStream is = this.fs.open(filePath)) {
			final Iterator<String> iterator = new BaseCollectorIterator(is, new AggregatorReport());
			final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
			return StreamSupport.stream(spliterator, false);
		} catch (final Throwable e) {
			throw new CollectorException(e);
		}
	}

}
