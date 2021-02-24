
package eu.dnetlib.dhp.oa.graph.raw;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.raw.common.AbstractMigrationApplication;
import eu.dnetlib.dhp.common.MdstoreClient;

public class MigrateMongoMdstoresApplication extends AbstractMigrationApplication
	implements Closeable {

	private static final Log log = LogFactory.getLog(MigrateMongoMdstoresApplication.class);

	private final MdstoreClient mdstoreClient;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateMongoMdstoresApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/migrate_mongo_mstores_parameters.json")));
		parser.parseArgument(args);

		final String mongoBaseUrl = parser.get("mongoBaseUrl");
		final String mongoDb = parser.get("mongoDb");

		final String mdFormat = parser.get("mdFormat");
		final String mdLayout = parser.get("mdLayout");
		final String mdInterpretation = parser.get("mdInterpretation");

		final String hdfsPath = parser.get("hdfsPath");

		try (MigrateMongoMdstoresApplication app = new MigrateMongoMdstoresApplication(hdfsPath, mongoBaseUrl,
			mongoDb)) {
			app.execute(mdFormat, mdLayout, mdInterpretation);
		}
	}

	public MigrateMongoMdstoresApplication(
		final String hdfsPath, final String mongoBaseUrl, final String mongoDb) throws Exception {
		super(hdfsPath);
		this.mdstoreClient = new MdstoreClient(mongoBaseUrl, mongoDb);
	}

	public void execute(final String format, final String layout, final String interpretation) {
		final Map<String, String> colls = mdstoreClient.validCollections(format, layout, interpretation);
		log.info("Found " + colls.size() + " mdstores");

		for (final Entry<String, String> entry : colls.entrySet()) {
			log.info("Processing mdstore " + entry.getKey() + " (collection: " + entry.getValue() + ")");
			final String currentColl = entry.getValue();

			for (final String xml : mdstoreClient.listRecords(currentColl)) {
				emit(xml, String.format("%s-%s-%s", format, layout, interpretation));
			}
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		mdstoreClient.close();
	}
}
