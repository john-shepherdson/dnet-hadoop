
package eu.dnetlib.dhp.collection.plugin.base;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

public class BaseCollectorIterator implements Iterator<Document> {

	private Object nextElement;

	private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>(20);

	private static final Logger log = LoggerFactory.getLogger(BaseCollectorIterator.class);

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
		return (this.nextElement != null) && (this.nextElement instanceof Document);
	}

	@Override
	public synchronized Document next() {
		try {
			return this.nextElement instanceof Document ? (Document) this.nextElement : null;
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

						final String xml = IOUtils.toString(new InputStreamReader(bzipInput));

						final Document doc = DocumentHelper.parseText(xml);

						for (final Object o : doc
							.selectNodes("//*[local-name()='ListRecords']/*[local-name()='record']")) {
							if (o instanceof Element) {
								final Element newRoot = (Element) ((Element) o).detach();
								final Document newDoc = DocumentHelper.createDocument(newRoot);
								this.queue.put(newDoc);
								count++;
							}
						}
					}
				}
			}

			this.queue.put("__END__"); // I ADD A NOT ELEMENT OBJECT TO INDICATE THE END OF THE QUEUE
		} catch (final Throwable e) {
			log.error("Error processing BASE records", e);
			report.put(e.getClass().getName(), e.getMessage());
			throw new RuntimeException("Error processing BASE records", e);
		} finally {
			log.info("Total records (written in queue): " + count);
		}
	}

}
