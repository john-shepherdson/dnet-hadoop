
package eu.dnetlib.dhp.collection.plugin.mongodb;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.MdstoreClient;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class MDStoreCollectorPlugin implements CollectorPlugin {

	private static final Logger log = LoggerFactory.getLogger(MDStoreCollectorPlugin.class);

	public static final String MONGODB_DBNAME = "mongodb_dbname";
	public static final String MDSTORE_ID = "mdstore_id";

	@Override
	public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {

		final String mongoBaseUrl = Optional
			.ofNullable(api.getBaseUrl())
			.orElseThrow(
				() -> new CollectorException(
					"missing mongodb baseUrl, expected in eu.dnetlib.dhp.collection.ApiDescriptor.baseUrl"));
		log.info("mongoBaseUrl: {}", mongoBaseUrl);

		final String dbName = Optional
			.ofNullable(api.getParams().get(MONGODB_DBNAME))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MONGODB_DBNAME)));
		log.info("dbName: {}", dbName);

		final String mdId = Optional
			.ofNullable(api.getParams().get(MDSTORE_ID))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MDSTORE_ID)));
		log.info("mdId: {}", mdId);

		final MdstoreClient client = new MdstoreClient(mongoBaseUrl, dbName);
		final MongoCollection<Document> mdstore = client.mdStore(mdId);
		long size = mdstore.count();

		return StreamSupport
			.stream(
				Spliterators.spliterator(mdstore.find().iterator(), size, Spliterator.SIZED), false)
			.map(doc -> doc.getString("body"));
	}
}
