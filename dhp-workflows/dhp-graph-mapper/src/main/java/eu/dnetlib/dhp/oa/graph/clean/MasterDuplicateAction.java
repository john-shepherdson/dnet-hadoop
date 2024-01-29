
package eu.dnetlib.dhp.oa.graph.clean;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.action.ReadDatasourceMasterDuplicateFromDB;

public class MasterDuplicateAction {

	private static final Logger log = LoggerFactory.getLogger(MasterDuplicateAction.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MasterDuplicateAction.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/datasourcemaster_parameters.json")));

		parser.parseArgument(args);

		final String dbUrl = parser.get("postgresUrl");
		log.info("postgresUrl: {}", dbUrl);

		final String dbUser = parser.get("postgresUser");
		log.info("postgresUser: {}", dbUser);

		final String dbPassword = parser.get("postgresPassword");
		log.info("postgresPassword: {}", dbPassword);

		final String hdfsPath = parser.get("hdfsPath");
		log.info("hdfsPath: {}", hdfsPath);

		final String hdfsNameNode = parser.get("hdfsNameNode");
		log.info("hdfsNameNode: {}", hdfsNameNode);

		int rows = ReadDatasourceMasterDuplicateFromDB.execute(dbUrl, dbUser, dbPassword, hdfsPath, hdfsNameNode);

		log.info("written {} rows", rows);
	}

}
