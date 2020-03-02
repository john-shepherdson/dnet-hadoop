package eu.dnetlib.dhp.migration.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import com.google.common.collect.Iterables;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MdstoreClient implements Closeable {

	private final MongoClient client;
	private final MongoDatabase db;

	private static final String COLL_METADATA = "metadata";
	private static final String COLL_METADATA_MANAGER = "metadataManager";

	private static final Log log = LogFactory.getLog(MdstoreClient.class);

	public MdstoreClient(final String baseUrl, final String dbName) {
		this.client = new MongoClient(new MongoClientURI(baseUrl));
		this.db = getDb(client, dbName);
	}

	public Map<String, String> validCollections(final String mdFormat, final String mdLayout, final String mdInterpretation) {

		final Map<String, String> transactions = new HashMap<>();
		for (final Document entry : getColl(db, COLL_METADATA_MANAGER, true).find()) {
			final String mdId = entry.getString("mdId");
			final String currentId = entry.getString("currentId");
			if (StringUtils.isNoneBlank(mdId, currentId)) {
				transactions.put(mdId, currentId);
			}
		}

		final Map<String, String> res = new HashMap<>();
		for (final Document entry : getColl(db, COLL_METADATA, true).find()) {
			if (entry.getString("format").equals(mdFormat) && entry.getString("layout").equals(mdLayout)
					&& entry.getString("interpretation").equals(mdInterpretation) && transactions.containsKey(entry.getString("mdId"))) {
				res.put(entry.getString("mdId"), transactions.get(entry.getString("mdId")));
			}
		}

		return res;
	}

	private MongoDatabase getDb(final MongoClient client, final String dbName) {
		if (!Iterables.contains(client.listDatabaseNames(), dbName)) {
			final String err = String.format("Database '%s' not found in %s", dbName, client.getAddress());
			log.warn(err);
			throw new RuntimeException(err);
		}
		return client.getDatabase(dbName);
	}

	private MongoCollection<Document> getColl(final MongoDatabase db, final String collName, final boolean abortIfMissing) {
		if (!Iterables.contains(db.listCollectionNames(), collName)) {
			final String err = String.format(String.format("Missing collection '%s' in database '%s'", collName, db.getName()));
			log.warn(err);
			if (abortIfMissing) {
				throw new RuntimeException(err);
			} else {
				return null;
			}
		}
		return db.getCollection(collName);
	}

	public Iterable<String> listRecords(final String collName) {
		final MongoCollection<Document> coll = getColl(db, collName, false);
		return coll == null ? new ArrayList<>()
				: () -> StreamSupport.stream(coll.find().spliterator(), false)
						.filter(e -> e.containsKey("body"))
						.map(e -> e.getString("body"))
						.iterator();
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

}
