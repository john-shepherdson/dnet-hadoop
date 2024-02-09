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

public class BaseCollectorIterator implements Iterator<Element> {

	private final BlockingQueue<Element> queue = new LinkedBlockingQueue<>();

	private static final Logger log = LoggerFactory.getLogger(BaseCollectorIterator.class);

	private boolean completed = false;

	public BaseCollectorIterator(final FileSystem fs, final Path filePath, final AggregatorReport report) {
		new Thread(() -> importHadoopFile(fs, filePath, report)).start();
	}

	protected BaseCollectorIterator(final String resourcePath, final AggregatorReport report) {
		new Thread(() -> importTestFile(resourcePath, report)).start();
	}

	@Override
	public synchronized boolean hasNext() {
		return !this.queue.isEmpty() || !isCompleted();
	}

	@Override
	public synchronized Element next() {
		try {
			return this.queue.take();
		} catch (final InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private void importHadoopFile(final FileSystem fs, final Path filePath, final AggregatorReport report) {
		log.info("I start to read the TAR stream");

		try (InputStream origInputStream = fs.open(filePath);
				final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(origInputStream)) {
			importTarStream(tarInputStream);
		} catch (final Throwable e) {
			log.error("Error processing BASE records", e);
			report.put(e.getClass().getName(), e.getMessage());
			throw new RuntimeException("Error processing BASE records", e);
		} finally {
			setCompleted(true);
		}
	}

	private void importTestFile(final String resourcePath, final AggregatorReport report) {
		try (final InputStream origInputStream = BaseCollectorIterator.class.getResourceAsStream(resourcePath);
				final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(origInputStream)) {
			importTarStream(tarInputStream);
		} catch (final Throwable e) {
			log.error("Error processing BASE records", e);
			report.put(e.getClass().getName(), e.getMessage());
			throw new RuntimeException("Error processing BASE records", e);
		} finally {
			setCompleted(true);
		}
	}

	private void importTarStream(final TarArchiveInputStream tarInputStream) throws Exception {
		TarArchiveEntry entry;
		long count = 0;
		while ((entry = (TarArchiveEntry) tarInputStream.getNextEntry()) != null) {
			final String name = entry.getName();

			if (!entry.isDirectory() && name.contains("ListRecords") && name.endsWith(".bz2")) {

				log.info("Processing file (BZIP): " + name);

				final byte[] bzipData = new byte[(int) entry.getSize()];
				IOUtils.readFully(tarInputStream, bzipData);

				try (InputStream bzipIs = new ByteArrayInputStream(bzipData);
						final BufferedInputStream bzipBis = new BufferedInputStream(bzipIs);
						final CompressorInputStream bzipInput = new CompressorStreamFactory().createCompressorInputStream(bzipBis)) {

					final String xml = IOUtils.toString(new InputStreamReader(bzipInput));

					final Document doc = DocumentHelper.parseText(xml);

					for (final Object o : doc.selectNodes("//*[local-name()='ListRecords']/*[local-name()='record']")) {
						if (o instanceof Element) {
							this.queue.add((Element) o);
							count++;
						}
					}
				}
			}
		}
		log.info("Total records (written in queue): " + count);
	}

	private synchronized boolean isCompleted() {
		return this.completed;
	}

	private synchronized void setCompleted(final boolean completed) {
		this.completed = completed;
	}

}
