
package eu.dnetlib.dhp.common;

import static com.mongodb.client.model.Sorts.descending;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.QueryBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MdstoreClient implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(MdstoreClient.class);

	private final MongoClient client;
	private final MongoDatabase db;

	private static final String COLL_METADATA = "metadata";
	private static final String COLL_METADATA_MANAGER = "metadataManager";

	public MdstoreClient(final String baseUrl, final String dbName) {
		this.client = new MongoClient(new MongoClientURI(baseUrl));
		this.db = getDb(client, dbName);
	}

	private Long parseTimestamp(Document f) {
		if (f == null || !f.containsKey("timestamp"))
			return null;

		Object ts = f.get("timestamp");

		return Long.parseLong(ts.toString());
	}

	public Long getLatestTimestamp(final String collectionId) {
		MongoCollection<Document> collection = db.getCollection(collectionId);
		FindIterable<Document> result = collection.find().sort(descending("timestamp")).limit(1);
		if (result == null) {
			return null;
		}

		Document f = result.first();
		return parseTimestamp(f);
	}

	public MongoCollection<Document> mdStore(final String mdId) {
		BasicDBObject query = (BasicDBObject) QueryBuilder.start("mdId").is(mdId).get();

		log.info("querying current mdId: {}", query.toJson());

		final String currentId = Optional
			.ofNullable(getColl(db, COLL_METADATA_MANAGER, true).find(query))
			.map(FindIterable::first)
			.map(d -> d.getString("currentId"))
			.orElseThrow(() -> new IllegalArgumentException("cannot find current mdstore id for: " + mdId));

		log.info("currentId: {}", currentId);

		return getColl(db, currentId, true);
	}

	public List<MDStoreInfo> mdStoreWithTimestamp(final String mdFormat, final String mdLayout,
		final String mdInterpretation) {
		Map<String, String> res = validCollections(mdFormat, mdLayout, mdInterpretation);
		return res
			.entrySet()
			.stream()
			.map(e -> new MDStoreInfo(e.getKey(), e.getValue(), getLatestTimestamp(e.getValue())))
			.collect(Collectors.toList());
	}

	public Map<String, String> validCollections(
		final String mdFormat, final String mdLayout, final String mdInterpretation) {

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
			if (entry.getString("format").equals(mdFormat)
				&& entry.getString("layout").equals(mdLayout)
				&& entry.getString("interpretation").equals(mdInterpretation)
				&& transactions.containsKey(entry.getString("mdId"))) {
				res.put(entry.getString("mdId"), transactions.get(entry.getString("mdId")));
			}
		}

		return res;
	}

	private MongoDatabase getDb(final MongoClient client, final String dbName) {
		if (!Iterables.contains(client.listDatabaseNames(), dbName)) {
			final String err = String.format("Database '%s' not found in %s", dbName, client.getAddress());
			log.warn(err);
			throw new IllegalArgumentException(err);
		}
		return client.getDatabase(dbName);
	}

	private MongoCollection<Document> getColl(
		final MongoDatabase db, final String collName, final boolean abortIfMissing) {
		if (!Iterables.contains(db.listCollectionNames(), collName)) {
			final String err = String
				.format(
					String.format("Missing collection '%s' in database '%s'", collName, db.getName()));
			log.warn(err);
			if (abortIfMissing) {
				throw new IllegalArgumentException(err);
			} else {
				return null;
			}
		}
		return db.getCollection(collName);
	}

	public Iterable<String> listRecords(final String collName) {
		final MongoCollection<Document> coll = getColl(db, collName, false);
		return coll == null
			? new ArrayList<>()
			: () -> StreamSupport
				.stream(coll.find().spliterator(), false)
				.filter(e -> e.containsKey("body"))
				.map(e -> e.getString("body"))
				.iterator();
	}

	@Override
	public void close() throws IOException {
		client.close();
	}
}
