
package eu.dnetlib.dhp.collection.plugin.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class FileCsvCollectorPlugin implements CollectorPlugin {

	private static final Logger log = LoggerFactory.getLogger(FileCsvCollectorPlugin.class);

	// TODO USE HDFS FILESSYSTEM
	private final FileSystem fileSystem;

	public FileCsvCollectorPlugin(final FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {

		final Path filePath = Optional
				.ofNullable(api.getBaseUrl())
				.map(Path::new)
				.orElseThrow(() -> new CollectorException("missing baseUrl"));

		final boolean withHeaders = BooleanUtils.toBoolean(api.getParams().get("header"));
		final String separator = api.getParams().get("separator");
		final int identifierNumber = NumberUtils.toInt(api.getParams().get("identifier"), 0);
		final String quote = api.getParams().get("quote");

		final String[] headers;

		try (InputStream is = this.fileSystem.open(filePath);
				BOMInputStream bomis = new BOMInputStream(is);
				InputStreamReader isr = new InputStreamReader(bomis);
				BufferedReader br = new BufferedReader(isr)) {

			if (withHeaders) {
				final String[] tmpHeader = br.readLine().split(separator);
				if (StringUtils.isNotBlank(quote)) {
					int i = 0;
					headers = new String[tmpHeader.length];
					for (final String h : tmpHeader) {
						headers[i] = StringUtils.strip(h, quote);
						i++;
					}
				} else {
					headers = tmpHeader;
				}
			} else {
				headers = null;
			}

			final Iterator<String> iterator = new Iterator<String>() {

				private String next = calculateNext();

				@Override
				public boolean hasNext() {
					return this.next != null;
				}

				@Override
				public String next() {
					try {
						return new String(this.next);
					} finally {
						this.next = calculateNext();
					}
				}

				private String calculateNext() {
					try {
						final Document document = DocumentHelper.createDocument();
						final Element root = document.addElement("csvRecord");

						String newLine = br.readLine();

						// FIX: FOR SOME FILES IT RETURN NULL ALSO IF THE FILE IS NOT READY DONE
						if (newLine == null) {
							newLine = br.readLine();
						}
						// END FIX

						if (newLine != null) {
							final String[] currentRow = StringUtils.split(newLine, separator);

							if (currentRow != null) {

								for (int i = 0; i < currentRow.length; i++) {
									final String hAttribute = (headers != null) && (i < headers.length) ? headers[i] : "column" + i;

									final Element row = root.addElement("column");
									if (i == identifierNumber) {
										row.addAttribute("isID", "true");
									}
									final String value = StringUtils.isBlank(quote) ? currentRow[i] : StringUtils.strip(currentRow[i], quote);

									row.addAttribute("name", hAttribute).addText(value);
								}
								return document.asXML();
							}
						}
					} catch (final IOException e) {
						log.error("Error calculating next csv element", e);
					}

					return null;
				}
			};

			final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);

			return StreamSupport.stream(spliterator, false);

		} catch (final Throwable e) {
			throw new CollectorException(e);
		}

	}

}
