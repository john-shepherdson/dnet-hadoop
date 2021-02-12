
package eu.dnetlib.dhp.collection.plugin.mongodb;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.CollectorException;
import eu.dnetlib.dhp.collection.CollectorPluginReport;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;

public class MongoDbCollectorPlugin implements CollectorPlugin {

	public static final String MONGODB_HOST = "mongodb_host";
	public static final String MONGODB_PORT = "mongodb_port";
	public static final String MONGODB_COLLECTION = "mongodb_collection";
	public static final String MONGODB_DBNAME = "mongodb_dbname";

	@Override
	public Stream<String> collect(ApiDescriptor api, CollectorPluginReport report) throws CollectorException {

		final String host = Optional
			.ofNullable(api.getParams().get(MONGODB_HOST))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MONGODB_HOST)));

		final Integer port = Optional
			.ofNullable(api.getParams().get(MONGODB_PORT))
			.map(Integer::parseInt)
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MONGODB_PORT)));

		final String dbName = Optional
			.ofNullable(api.getParams().get(MONGODB_DBNAME))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MONGODB_DBNAME)));

		final String collection = Optional
			.ofNullable(api.getParams().get(MONGODB_COLLECTION))
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", MONGODB_COLLECTION)));

		final MongoClient mongoClient = new MongoClient(host, port);
		final MongoDatabase database = mongoClient.getDatabase(dbName);
		final MongoCollection<Document> mdstore = database.getCollection(collection);

		long size = mdstore.count();

		return StreamSupport
			.stream(
				Spliterators.spliterator(mdstore.find().iterator(), size, Spliterator.SIZED), false)
			.map(doc -> doc.getString("body"));
	}
}
