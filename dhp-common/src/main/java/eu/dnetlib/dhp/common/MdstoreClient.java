
package eu.dnetlib.dhp.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class MdstoreClient implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(MdstoreClient.class);

	private final MongoClient client;
	private final MongoDatabase db;

	public static final String MD_ID = "mdId";
	public static final String CURRENT_ID = "currentId";
	public static final String EXPIRING = "expiring";
	public static final String ID = "id";
	public static final String LAST_READ = "lastRead";

	public static final String FORMAT = "format";
	public static final String LAYOUT = "layout";
	public static final String INTERPRETATION = "interpretation";

	public static final String BODY = "body";

	private static final String COLL_METADATA = "metadata";
	private static final String COLL_METADATA_MANAGER = "metadataManager";

	public MdstoreClient(final MongoClient mongoClient, final String dbName) {
		this.client = mongoClient;
		this.db = getDb(client, dbName);
	}

	public Iterable<String> mdStoreRecords(final String mdId) {
		return recordIterator(mdStore(mdId));
	}

	public MongoCollection<Document> mdStore(final String mdId) {
		final Document mdStoreInfo = getMDStoreInfo(mdId);
		final String currentId = mdStoreInfo.getString(CURRENT_ID);
		log.info("reading currentId: {}", currentId);

		return getColl(db, currentId, true);
	}

	public MdstoreTx readLock(final String mdId) {

		final Document mdStoreInfo = getMDStoreInfo(mdId);
		final List expiring = mdStoreInfo.get(EXPIRING, List.class);
		final String currentId = mdStoreInfo.getString(CURRENT_ID);

		log.info("locking collection {}", currentId);

		if (expiring.size() > 0) {
			for (Object value : expiring) {
				final Document obj = (Document) value;
				final String expiringId = (String) obj.get(ID);
				if (currentId.equals(expiringId)) {
					obj.put(LAST_READ, new Date());
					break;
				}
			}
		} else {
			final BasicDBObject readStore = new BasicDBObject();
			readStore.put(ID, currentId);
			readStore.put(LAST_READ, new Date());
			expiring.add(readStore);
		}

		getColl(db, COLL_METADATA_MANAGER, true)
			.findOneAndReplace(new BasicDBObject("_id", mdStoreInfo.get("_id")), mdStoreInfo);

		return new MdstoreTx(this, mdId, currentId);
	}

	public void readUnlock(final String mdId, final String currentId) {

		log.info("unlocking collection {}", currentId);

		final Document mdStoreInfo = getMDStoreInfo(mdId);
		final List<Document> expiring = mdStoreInfo.get(EXPIRING, List.class);

		expiring
			.stream()
			.filter(d -> currentId.equals(d.getString(ID)))
			.findFirst()
			.ifPresent(expired -> expiring.remove(expired));
	}

	/**
	 * Retrieves from the MDStore mongoDB database a snapshot of the [mdID, currentID] pairs.
	 * @param mdFormat
	 * @param mdLayout
	 * @param mdInterpretation
	 * @return an HashMap of the mdID -> currentID associations.
	 */
	public Map<String, String> validCollections(
		final String mdFormat, final String mdLayout, final String mdInterpretation) {

		final Map<String, String> transactions = new HashMap<>();
		for (final Document entry : getColl(db, COLL_METADATA_MANAGER, true).find()) {
			final String mdId = entry.getString(MD_ID);
			final String currentId = entry.getString(CURRENT_ID);
			if (StringUtils.isNoneBlank(mdId, currentId)) {
				transactions.put(mdId, currentId);
			}
		}

		final Map<String, String> res = new HashMap<>();
		for (final Document entry : getColl(db, COLL_METADATA, true).find()) {
			if (entry.getString(FORMAT).equals(mdFormat)
				&& entry.getString(LAYOUT).equals(mdLayout)
				&& entry.getString(INTERPRETATION).equals(mdInterpretation)
				&& transactions.containsKey(entry.getString(MD_ID))) {
				res.put(entry.getString(MD_ID), transactions.get(entry.getString(MD_ID)));
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

	private MongoCollection<Document> getColl(
		final MongoDatabase db, final String collName, final boolean abortIfMissing) {
		if (!Iterables.contains(db.listCollectionNames(), collName)) {
			final String err = String.format("Missing collection '%s' in database '%s'", collName, db.getName());
			log.warn(err);
			if (abortIfMissing) {
				throw new RuntimeException(err);
			} else {
				return null;
			}
		}
		return db.getCollection(collName);
	}

	private Document getMDStoreInfo(final String mdId) {
		return Optional
			.ofNullable(getColl(db, COLL_METADATA_MANAGER, true))
			.map(metadataManager -> {
				BasicDBObject query = (BasicDBObject) QueryBuilder.start(MD_ID).is(mdId).get();
				log.info("querying current mdId: {}", query.toJson());
				return Optional
					.ofNullable(metadataManager.find(query))
					.map(MongoIterable::first)
					.orElseThrow(() -> new IllegalArgumentException("cannot find current mdstore id for: " + mdId));
			})
			.orElseThrow(() -> new IllegalStateException("missing collection " + COLL_METADATA_MANAGER));
	}

	public Iterable<String> listRecords(final String collName) {
		return recordIterator(getColl(db, collName, false));
	}

	private Iterable<String> recordIterator(MongoCollection<Document> coll) {
		return coll == null
			? new ArrayList<>()
			: () -> StreamSupport
				.stream(coll.find().spliterator(), false)
				.filter(e -> e.containsKey(BODY))
				.map(e -> e.getString(BODY))
				.iterator();
	}

	@Override
	public void close() throws IOException {
		client.close();
	}
}
