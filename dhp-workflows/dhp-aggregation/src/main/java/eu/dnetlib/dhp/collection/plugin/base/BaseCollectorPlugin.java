
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
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
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
		final String acceptedNormTypesString = api.getParams().get("acceptedNormTypes");

		log.info("baseUrl: {}", filePath);
		log.info("dbUrl: {}", dbUrl);
		log.info("dbUser: {}", dbUser);
		log.info("dbPassword: {}", "***");
		log.info("acceptedNormTypes: {}", acceptedNormTypesString);

		try {
			if (!this.fs.exists(filePath)) {
				throw new CollectorException("path does not exist: " + filePath);
			}
		} catch (final Throwable e) {
			throw new CollectorException(e);
		}

		final Set<String> acceptedOpendoarIds = findAcceptedOpendoarIds(dbUrl, dbUser, dbPassword);

		final Set<String> acceptedNormTypes = new HashSet<>();
		if (StringUtils.isNotBlank(acceptedNormTypesString)) {
			for (final String s : StringUtils.split(acceptedNormTypesString, ",")) {
				if (StringUtils.isNotBlank(s)) {
					acceptedNormTypes.add(s.trim());
				}
			}
		}

		final Iterator<String> iterator = new BaseCollectorIterator(this.fs, filePath, report);
		final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
		return StreamSupport
			.stream(spliterator, false)
			.filter(doc -> filterXml(doc, acceptedOpendoarIds, acceptedNormTypes));
	}

	private Set<String> findAcceptedOpendoarIds(final String dbUrl, final String dbUser, final String dbPassword)
		throws CollectorException {
		final Set<String> accepted = new HashSet<>();

		try (final DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {

			final String sql = IOUtils
				.toString(
					getClass().getResourceAsStream("/eu/dnetlib/dhp/collection/plugin/base/sql/opendoar-accepted.sql"));

			dbClient.processResults(sql, row -> {
				try {
					final String dsId = row.getString("id");
					log.info("Accepted Datasource: " + dsId);
					accepted.add(dsId);
				} catch (final SQLException e) {
					log.error("Error in SQL", e);
					throw new RuntimeException("Error in SQL", e);
				}
			});

		} catch (final IOException e) {
			log.error("Error accessong SQL", e);
			throw new CollectorException("Error accessong SQL", e);
		}

		log.info("Accepted Datasources (TOTAL): " + accepted.size());

		return accepted;
	}

	protected static boolean filterXml(final String xml,
		final Set<String> acceptedOpendoarIds,
		final Set<String> acceptedNormTypes) {
		try {

			final Document doc = DocumentHelper.parseText(xml);

			final String id = doc.valueOf("//*[local-name()='collection']/@opendoar_id").trim();

			if (StringUtils.isBlank(id) || !acceptedOpendoarIds.contains("opendoar____::" + id)) {
				return false;
			}

			if (acceptedNormTypes.isEmpty()) {
				return true;
			}

			for (final Object s : doc.selectNodes("//*[local-name()='typenorm']")) {
				if (acceptedNormTypes.contains(((Node) s).getText().trim())) {
					return true;
				}
			}

			return false;
		} catch (final DocumentException e) {
			log.error("Error parsing document", e);
			throw new RuntimeException("Error parsing document", e);
		}
	}

}
