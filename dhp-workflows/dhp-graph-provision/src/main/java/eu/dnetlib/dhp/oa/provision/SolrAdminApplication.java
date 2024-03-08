
package eu.dnetlib.dhp.oa.provision;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.dhp.oa.provision.utils.ZkServers;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;

public class SolrAdminApplication implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(SolrAdminApplication.class);

	enum Action {
		DELETE_BY_QUERY, COMMIT
	}

	private final CloudSolrClient solrClient;

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SolrAdminApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_solradmin_parameters.json")));
		parser.parseArgument(args);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String format = parser.get("format");
		log.info("format: {}", format);

		final Action action = Action.valueOf(parser.get("action"));
		log.info("action: {}", action);

		final String query = parser.get("query");
		log.info("query: {}", query);

		final boolean commit = Optional
			.ofNullable(parser.get("commit"))
			.map(Boolean::valueOf)
			.orElse(false);
		log.info("commit: {}", commit);

		final ISLookupClient isLookup = new ISLookupClient(ISLookupClientFactory.getLookUpService(isLookupUrl));

		final String zkHost = isLookup.getZkHost();
		log.info("zkHost: {}", zkHost);

		final String collection = ProvisionConstants.getCollectionName(format);
		log.info("collection: {}", collection);

		final boolean shouldIndex = Optional
			.ofNullable(parser.get("shouldIndex"))
			.map(Boolean::valueOf)
			.orElse(false);
		log.info("shouldIndex: {}", shouldIndex);

		if (shouldIndex) {
			try (SolrAdminApplication app = new SolrAdminApplication(zkHost)) {
				app.execute(action, collection, query, commit);
			}
		}
	}

	public SolrAdminApplication(String zkHost) {
		final ZkServers zk = ZkServers.newInstance(zkHost);
		this.solrClient = new CloudSolrClient.Builder(zk.getHosts(), zk.getChroot()).build();
	}

	public SolrResponse commit(String collection) throws IOException, SolrServerException {
		return execute(Action.COMMIT, collection, null, true);
	}

	public SolrResponse execute(Action action, String collection, String query, boolean commit)
		throws IOException, SolrServerException {
		switch (action) {

			case DELETE_BY_QUERY:
				UpdateResponse rsp = solrClient.deleteByQuery(collection, query);
				if (commit) {
					solrClient.commit(collection);
				}
				return rsp;
			case COMMIT:
				return solrClient.commit(collection);
			default:
				throw new IllegalArgumentException("action not managed: " + action);
		}
	}

	@Override
	public void close() throws IOException {
		solrClient.close();
	}

}
