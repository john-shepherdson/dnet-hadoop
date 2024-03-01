
package eu.dnetlib.dhp.collection.plugin.base;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.file.AbstractSplittedRecordPlugin;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class BaseCollectorPlugin implements CollectorPlugin {

	private final FileSystem fs;

	private static final Logger log = LoggerFactory.getLogger(AbstractSplittedRecordPlugin.class);

	// MAPPING AND FILTERING ARE DEFINED HERE:
	// https://docs.google.com/document/d/1Aj-ZAV11b44MCrAAUCPiS2TUlXb6PnJEu1utCMAcCOU/edit

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

		final String dbUrl = api.getParams().get("dbUrl");
		final String dbUser = api.getParams().get("dbUser");
		final String dbPassword = api.getParams().get("dbPassword");

		log.info("baseUrl: {}", filePath);
		log.info("dbUrl: {}", dbUrl);
		log.info("dbUser: {}", dbUser);
		log.info("dbPassword: {}", "***");

		try {
			if (!this.fs.exists(filePath)) { throw new CollectorException("path does not exist: " + filePath); }
		} catch (final Throwable e) {
			throw new CollectorException(e);
		}

		final Set<String> excludedOpendoarIds = findExcludedOpendoarIds(dbUrl, dbUser, dbPassword);

		final Iterator<String> iterator = new BaseCollectorIterator(this.fs, filePath, report);
		final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
		return StreamSupport
				.stream(spliterator, false)
				.filter(doc -> filterXml(doc, excludedOpendoarIds, report));
	}

	private Set<String> findExcludedOpendoarIds(final String dbUrl, final String dbUser, final String dbPassword) throws CollectorException {
		final Set<String> excluded = new HashSet<>();

		try (final DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {

			final String sql = IOUtils
					.toString(BaseAnalyzerJob.class
							.getResourceAsStream("/eu/dnetlib/dhp/collection/plugin/base/sql/opendoar-ds-exclusion.sql"));

			dbClient.processResults(sql, row -> {
				try {
					excluded.add(row.getString("id"));
				} catch (final SQLException e) {
					log.error("Error in SQL", e);
					throw new RuntimeException("Error in SQL", e);
				}
			});
		} catch (final IOException e) {
			log.error("Error accessong SQL", e);
			throw new CollectorException("Error accessong SQL", e);
		}
		return excluded;
	}

	private boolean filterXml(final String xml, final Set<String> excludedOpendoarIds, final AggregatorReport report) {
		try {
			final String id = DocumentHelper.parseText(xml).valueOf("//*[local-name='collection']/@opendoar_id").trim;
			return (StringUtils.isNotBlank(id) && !excludedOpendoarIds.contains("opendoar____::" + id.trim()));
		} catch (final DocumentException e) {
			log.error("Error parsing document", e);
			throw new RuntimeException("Error parsing document", e);
		}
	}

}
