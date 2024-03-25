
package eu.dnetlib.dhp.collection.plugin.base;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

public class BaseCollectorIterator implements Iterator<String> {

	private String nextElement;

	private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(100);

	private static final Logger log = LoggerFactory.getLogger(BaseCollectorIterator.class);

	private static final String END_ELEM = "__END__";

	public BaseCollectorIterator(final FileSystem fs, final Path filePath, final AggregatorReport report) {
		new Thread(() -> importHadoopFile(fs, filePath, report)).start();
		try {
			this.nextElement = this.queue.take();
		} catch (final InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	protected BaseCollectorIterator(final String resourcePath, final AggregatorReport report) {
		new Thread(() -> importTestFile(resourcePath, report)).start();
		try {
			this.nextElement = this.queue.take();
		} catch (final InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized boolean hasNext() {
		return (this.nextElement != null) & !END_ELEM.equals(this.nextElement);
	}

	@Override
	public synchronized String next() {
		try {
			return END_ELEM.equals(this.nextElement) ? null : this.nextElement;
		} finally {
			try {
				this.nextElement = this.queue.take();
			} catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

	}

	private void importHadoopFile(final FileSystem fs, final Path filePath, final AggregatorReport report) {
		log.info("I start to read the TAR stream");

		try (InputStream origInputStream = fs.open(filePath);
			final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(origInputStream)) {
			importTarStream(tarInputStream, report);
		} catch (final Throwable e) {
			throw new RuntimeException("Error processing BASE records", e);
		}
	}

	private void importTestFile(final String resourcePath, final AggregatorReport report) {
		try (final InputStream origInputStream = BaseCollectorIterator.class.getResourceAsStream(resourcePath);
			final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(origInputStream)) {
			importTarStream(tarInputStream, report);
		} catch (final Throwable e) {
			throw new RuntimeException("Error processing BASE records", e);
		}
	}

	private void importTarStream(final TarArchiveInputStream tarInputStream, final AggregatorReport report) {
		long count = 0;

		final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();

		try {
			TarArchiveEntry entry;
			while ((entry = (TarArchiveEntry) tarInputStream.getNextEntry()) != null) {
				final String name = entry.getName();

				if (!entry.isDirectory() && name.contains("ListRecords") && name.endsWith(".bz2")) {

					log.info("Processing file (BZIP): " + name);

					final byte[] bzipData = new byte[(int) entry.getSize()];
					IOUtils.readFully(tarInputStream, bzipData);

					try (InputStream bzipIs = new ByteArrayInputStream(bzipData);
						final BufferedInputStream bzipBis = new BufferedInputStream(bzipIs);
						final CompressorInputStream bzipInput = new CompressorStreamFactory()
							.createCompressorInputStream(bzipBis)) {

						final XMLEventReader reader = xmlInputFactory.createXMLEventReader(bzipInput);

						XMLEventWriter eventWriter = null;
						StringWriter xmlWriter = null;

						while (reader.hasNext()) {
							final XMLEvent nextEvent = reader.nextEvent();

							if (nextEvent.isStartElement()) {
								final StartElement startElement = nextEvent.asStartElement();
								if ("record".equals(startElement.getName().getLocalPart())) {
									xmlWriter = new StringWriter();
									eventWriter = xmlOutputFactory.createXMLEventWriter(xmlWriter);
								}
							}

							if (eventWriter != null) {
								eventWriter.add(nextEvent);
							}

							if (nextEvent.isEndElement()) {
								final EndElement endElement = nextEvent.asEndElement();
								if ("record".equals(endElement.getName().getLocalPart())) {
									eventWriter.flush();
									eventWriter.close();

									this.queue.put(xmlWriter.toString());

									eventWriter = null;
									xmlWriter = null;
									count++;
								}
							}

						}
					}
				}
			}

			this.queue.put(END_ELEM); // TO INDICATE THE END OF THE QUEUE
		} catch (final Throwable e) {
			log.error("Error processing BASE records", e);
			report.put(e.getClass().getName(), e.getMessage());
			throw new RuntimeException("Error processing BASE records", e);
		} finally {
			log.info("Total records (written in queue): " + count);
		}
	}

}
