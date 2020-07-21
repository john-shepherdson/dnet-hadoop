
package eu.dnetlib.dhp.oa.graph.raw;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;

public class MigrateDbEntitiesApplication extends AbstractDbApplication {

	private static final Logger log = LoggerFactory.getLogger(MigrateDbEntitiesApplication.class);

	public static final String SOURCE_TYPE = "source_type";
	public static final String TARGET_TYPE = "target_type";

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateDbEntitiesApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_db_entities_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		log.info("postgresUrl: {}", dbUrl);

		final String dbUser = parser.get("postgresUser");
		log.info("postgresUser: {}", dbUser);

		final String dbPassword = parser.get("postgresPassword");
		log.info("postgresPassword: xxx");

		final String dbSchema = parser.get("dbschema");
		log.info("dbSchema {}: " + dbSchema);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

		final boolean processClaims = parser.get("action") != null && parser.get("action").equalsIgnoreCase("claims");
		log.info("processClaims: {}", processClaims);

		try (final MigrateDbEntitiesApplication mapper = new MigrateDbEntitiesApplication(hdfsPath, dbUrl, dbUser,
			dbPassword, isLookupUrl)) {

			if (processClaims) {
				log.info("Processing claims...");
				mapper.execute("queryClaims.sql", mapper::processClaims);
			} else {
				log.info("Processing datasources...");
				mapper.execute("queryDatasources.sql", mapper::processDatasource);

				log.info("Processing projects...");
				if (dbSchema.equalsIgnoreCase("beta")) {
					mapper.execute("queryProjects.sql", mapper::processProject);
				} else {
					mapper.execute("queryProjects_production.sql", mapper::processProject);
				}

				log.info("Processing orgs...");
				mapper.execute("queryOrganizations.sql", mapper::processOrganization);

				log.info("Processing relationsNoRemoval ds <-> orgs ...");
				mapper.execute("queryDatasourceOrganization.sql", mapper::processDatasourceOrganization);

				log.info("Processing projects <-> orgs ...");
				mapper.execute("queryProjectOrganization.sql", mapper::processProjectOrganization);
			}
			log.info("All done.");
		}
	}

	public MigrateDbEntitiesApplication(final String hdfsPath, final String dbUrl, final String dbUser,
		final String dbPassword, final String isLookupUrl)
		throws Exception {
		super(hdfsPath, dbUrl, dbUser, dbPassword, isLookupUrl);
	}

	protected MigrateDbEntitiesApplication(final DbClient dbClient, final VocabularyGroup vocs) { // ONLY FOT TESTS
		super(dbClient, vocs);
	}

}
