package eu.dnetlib.dhp.collection.plugin.base;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class BaseCollectorIterator implements Iterator<String> {

	private final InputStream origInputStream;
	private final AggregatorReport report;
	private boolean completed;

	private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

	private static final Logger log = LoggerFactory.getLogger(BaseCollectorIterator.class);

	public BaseCollectorIterator(final InputStream origInputStream, final AggregatorReport report) {
		this.origInputStream = origInputStream;
		this.report = report;
		this.completed = false;

		new Thread(this::importFile).start();
	}

	@Override
	public boolean hasNext() {
		return !this.completed || !this.queue.isEmpty();
	}

	@Override
	public String next() {
		try {
			return this.queue.take();
		} catch (final InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private void importFile() {
		log.info("I start to read the TAR stream");

		long count = 0;
		try (final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(this.origInputStream)) {

			TarArchiveEntry entry;
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
							try (final StringWriter sw = new StringWriter()) {
								final XMLWriter writer = new XMLWriter(sw, OutputFormat.createPrettyPrint());
								writer.write((Node) o);

								this.queue.add(sw.toString());

								count += 1;
							} catch (final IOException e) {
								this.report.put(e.getClass().getName(), e.getMessage());
								throw new CollectorException("Error parsing XML record:\n" + ((Node) o).asXML(), e);
							}
						}
					}
				}
			}
		} catch (final Throwable e) {
			log.error("Error processing BASE records", e);
			throw new RuntimeException("Error processing BASE records", e);
		} finally {
			this.completed = true;
			log.info("Total records (written in queue): " + count);
		}
	}

}
