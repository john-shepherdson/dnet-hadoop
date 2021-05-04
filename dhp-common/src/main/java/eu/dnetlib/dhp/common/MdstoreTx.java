
package eu.dnetlib.dhp.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MdstoreTx implements Iterable<String>, Closeable {

	private static final Logger log = LoggerFactory.getLogger(MdstoreTx.class);

	private final MdstoreClient mdstoreClient;

	private final String mdId;

	private final String currentId;

	public MdstoreTx(MdstoreClient mdstoreClient, String mdId, String currentId) {
		this.mdstoreClient = mdstoreClient;
		this.mdId = mdId;
		this.currentId = currentId;
	}

	@Override
	public Iterator<String> iterator() {
		return mdstoreClient.mdStoreRecords(mdId).iterator();
	}

	@Override
	public void close() throws IOException {
		mdstoreClient.readUnlock(mdId, currentId);
		log.info("unlocked collection {}", currentId);
	}

	public String getMdId() {
		return mdId;
	}

	public String getCurrentId() {
		return currentId;
	}
}
