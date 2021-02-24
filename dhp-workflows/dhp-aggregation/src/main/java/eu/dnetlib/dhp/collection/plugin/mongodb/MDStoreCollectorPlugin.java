
package eu.dnetlib.dhp.collection.plugin.mongodb;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.mongodb.client.MongoCollection;
import eu.dnetlib.dhp.common.MdstoreClient;

import eu.dnetlib.dhp.aggregation.common.AggregatorReport;
import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.CollectorException;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import org.bson.Document;

public class MDStoreCollectorPlugin implements CollectorPlugin {

	public static final String MONGODB_BASEURL = "mongodb_baseurl";
	public static final String MONGODB_DBNAME = "mongodb_dbname";
	public static final String MDSTORE_ID = "mongodb_collection";

	@Override
	public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {

		final String mongoBaseUrl = Optional
			.ofNullable(api.getParams().get(MONGODB_BASEURL))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MONGODB_BASEURL)));

		final String dbName = Optional
			.ofNullable(api.getParams().get(MONGODB_DBNAME))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MONGODB_DBNAME)));

		final String mdId = Optional
			.ofNullable(api.getParams().get(MDSTORE_ID))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MDSTORE_ID)));

		final MdstoreClient client = new MdstoreClient(mongoBaseUrl, dbName);
		final MongoCollection<Document> mdstore = client.mdStore(mdId);
		long size = mdstore.count();

		return StreamSupport
			.stream(
				Spliterators.spliterator(mdstore.find().iterator(), size, Spliterator.SIZED), false)
			.map(doc -> doc.getString("body"));
	}
}
