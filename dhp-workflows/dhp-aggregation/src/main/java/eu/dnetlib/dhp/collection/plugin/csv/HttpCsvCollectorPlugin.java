
package eu.dnetlib.dhp.collection.plugin.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.utils.XmlCleaner;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;
import eu.dnetlib.dhp.utils.DHPUtils;

public class HttpCsvCollectorPlugin implements CollectorPlugin {

	private static final Logger log = LoggerFactory.getLogger(HttpCsvCollectorPlugin.class);

	// TODO USE HDFS FILESSYSTEM FOR TEMP FILE
	private final FileSystem fileSystem;

	private final HttpConnector2 httpConnector;

	public HttpCsvCollectorPlugin(final HttpClientParams clientParams, final FileSystem fileSystem) {
		this.httpConnector = new HttpConnector2(clientParams);
		this.fileSystem = fileSystem;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {

		final String baseUrl = api.getBaseUrl();

		final String separator = api.getParams().get("separator");
		final String identifier = api.getParams().get("identifier");
		final String quote = api.getParams().get("quote");

		long nLines = 0;

		try {
			// FIX
			// This code should skip the lines with invalid quotes
			final Path tempPath = new Path("/tmp/" + DHPUtils.md5(baseUrl) + ".csv.tmp");

			try (InputStream is = this.httpConnector.getInputSourceAsStream(baseUrl);
					BOMInputStream bomIs = new BOMInputStream(is);
					BufferedReader reader = new BufferedReader(new InputStreamReader(bomIs));
					FSDataOutputStream fsdos = this.fileSystem.create(tempPath, true);
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdos, StandardCharsets.UTF_8))) {

				String line;
				while ((line = reader.readLine()) != null) {
					if (StringUtils.isBlank(quote) || (quote.charAt(0) != '"') || verifyQuotes(line, separator.charAt(0))) {
						bw.write(line);
						bw.write("\n");
						nLines++;
					}
				}
			}
			// END FIX

			final CSVFormat format = CSVFormat.EXCEL
					.withHeader()
					.withDelimiter("\\t".equals(separator) || StringUtils.isBlank(separator) ? '\t' : separator.charAt(0))
					.withQuote(StringUtils.isBlank(quote) ? null : quote.charAt(0))
					.withTrim();

			try (InputStream is = this.fileSystem.open(tempPath);
					InputStreamReader isr = new InputStreamReader(is);
					BufferedReader br = new BufferedReader(isr);
					final CSVParser parser = new CSVParser(br, format)) {

				final Set<String> headers = parser.getHeaderMap().keySet();

				final long nRecords = nLines - 1;

				final Iterator<String> iterator = Iterators.transform(parser.iterator(), input -> {
					try {
						final Document document = DocumentHelper.createDocument();
						final Element root = document.addElement("csvRecord");
						for (final String key : headers) {
							final Element row = root.addElement("column");
							final String value = XmlCleaner.cleanAllEntities(input.get(key));
							if (value != null) {
								row.addAttribute("name", key).addText(value);
							}
							if (key.equals(identifier)) {
								row.addAttribute("isID", "true");
							}
						}

						return document.asXML();
					} finally {
						if (parser.getRecordNumber() == nRecords) {
							try {
								this.fileSystem.delete(tempPath, false);
							} catch (final IOException e) {
								log.warn("Error deleting temp file: " + tempPath);
							}
						}
					}
				});

				final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);

				return StreamSupport.stream(spliterator, false);
			}
		} catch (final Throwable e) {
			log.error("Error parsing csv", e);
			throw new CollectorException("Error parsing csv", e);
		}
	}

	public boolean verifyQuotes(final String line, final char separator) {
		final char[] cs = line.trim().toCharArray();
		boolean inField = false;
		boolean skipNext = false;
		for (int i = 0; i < cs.length; i++) {
			if (skipNext) {
				skipNext = false;
			} else if (inField) {
				if ((cs[i] == '\"') && ((i == (cs.length - 1)) || (cs[i + 1] == separator))) {
					inField = false;
				} else if ((cs[i] == '\"') && (i < (cs.length - 1))) {
					if (cs[i + 1] != '\"') {
						log.warn("Skipped invalid line: " + line);
						return false;
					}
					skipNext = true;
				}
			} else if ((cs[i] == '\"') && ((i == 0) || (cs[i - 1] == separator))) {
				inField = true;
			}
		}

		if (inField) {
			log.warn("Skipped invalid line: " + line);
			return false;
		}

		return true;
	}
}
