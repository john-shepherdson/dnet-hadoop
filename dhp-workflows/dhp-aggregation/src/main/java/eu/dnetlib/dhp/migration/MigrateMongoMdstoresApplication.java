package eu.dnetlib.dhp.migration;

import org.apache.commons.io.IOUtils;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class MigrateMongoMdstoresApplication {

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils.toString(MigrateMongoMdstoresApplication.class.getResourceAsStream("/eu/dnetlib/dhp/migration/migrate_mongo_mstores_parameters.json")));
		parser.parseArgument(args);

		final String mongoBaseUrl = parser.get("mongoBaseUrl");
		final String mongoDb = parser.get("mongoDb");

		final String mdFormat = parser.get("mdFormat");
		final String mdLayout = parser.get("mdLayout");
		final String mdInterpretation = parser.get("mdInterpretation");

		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("namenode");
		final String hdfsUser = parser.get("hdfsUser");

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		if (mdFormat.equalsIgnoreCase("oaf")) {
			try (final OafMigrationExecutor mig =
					new OafMigrationExecutor(hdfsPath, hdfsNameNode, hdfsUser, mongoBaseUrl, mongoDb, dbUrl, dbUser, dbPassword)) {
				mig.processMdRecords(mdFormat, mdLayout, mdInterpretation);
			}
		} else if (mdFormat.equalsIgnoreCase("oaf")) {

		} else {
			throw new RuntimeException("Format not supported: " + mdFormat);
		}

	}

}
