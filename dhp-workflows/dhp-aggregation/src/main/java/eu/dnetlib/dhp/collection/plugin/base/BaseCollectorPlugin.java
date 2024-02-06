package eu.dnetlib.dhp.collection.plugin.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
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

	// MAPPING AND FILTERING ARE DEFINED HERE: https://docs.google.com/document/d/1Aj-ZAV11b44MCrAAUCPiS2TUlXb6PnJEu1utCMAcCOU/edit

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
			final Iterator<Element> iterator = new BaseCollectorIterator(is);
			final Spliterator<Element> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
			return StreamSupport.stream(spliterator, false)
					.filter(elem -> filterXml(elem, report))
					.map(elem -> xmlToString(report, elem));
		} catch (final Throwable e) {
			report.put(e.getClass().getName(), e.getMessage());
			throw new CollectorException(e);
		}
	}

	private boolean filterXml(final Element elem, final AggregatorReport report) {
		// TODO Auto-generated method stub

		// HERE THE FILTERS ACCORDING TO THE DOCUMENTATION

		return true;
	}

	private String xmlToString(final AggregatorReport report, final Element elem) {
		try (final StringWriter sw = new StringWriter()) {
			final XMLWriter writer = new XMLWriter(sw, OutputFormat.createPrettyPrint());
			writer.write(elem);
			return writer.toString();
		} catch (final IOException e) {
			report.put(e.getClass().getName(), e.getMessage());
			throw new RuntimeException("Error indenting XML record", e);
		}
	}

}
