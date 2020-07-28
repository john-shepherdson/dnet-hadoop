
package eu.dnetlib.dhp.oa.graph.raw;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;

public class MigrateCordaOrgsApplication extends AbstractDbApplication {

	private static final Logger log = LoggerFactory.getLogger(MigrateCordaOrgsApplication.class);

	public static final String SOURCE_TYPE = "source_type";
	public static final String TARGET_TYPE = "target_type";

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateCordaOrgsApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_openorgs_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		log.info("postgresUrl: {}", dbUrl);

		final String dbUser = parser.get("postgresUser");
		log.info("postgresUser: {}", dbUser);

		final String dbPassword = parser.get("postgresPassword");
		log.info("postgresPassword: xxx");

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

		try (final MigrateCordaOrgsApplication mapper = new MigrateCordaOrgsApplication(hdfsPath, dbUrl, dbUser,
			dbPassword, isLookupUrl)) {

			log.info("Processing CORDA orgs...");
			mapper.execute("queryCordaOrganizations.sql", mapper::processOrganization);

		}
		log.info("All done.");

	}

	public MigrateCordaOrgsApplication(final String hdfsPath, final String dbUrl, final String dbUser,
		final String dbPassword, final String isLookupUrl)
		throws Exception {
		super(hdfsPath, dbUrl, dbUser, dbPassword, isLookupUrl);
	}

	protected MigrateCordaOrgsApplication(final DbClient dbClient, final VocabularyGroup vocs) { // ONLY FOT TESTS
		super(dbClient, vocs);
	}

}
