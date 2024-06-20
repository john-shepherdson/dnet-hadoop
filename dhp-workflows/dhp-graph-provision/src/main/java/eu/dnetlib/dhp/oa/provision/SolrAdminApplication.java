
package eu.dnetlib.dhp.oa.provision;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
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
		DELETE_BY_QUERY, COMMIT, UPDATE_ALIASES
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

		final String publicFormat = parser.get("publicFormat");
		log.info("publicFormat: {}", publicFormat);

		final String shadowFormat = parser.get("shadowFormat");
		log.info("shadowFormat: {}", shadowFormat);

		// get collection names from metadata format profiles names
		final String publicCollection = ProvisionConstants.getCollectionName(publicFormat);
		log.info("publicCollection: {}", publicCollection);

		final String shadowCollection = ProvisionConstants.getCollectionName(shadowFormat);
		log.info("shadowCollection: {}", shadowCollection);

		try (SolrAdminApplication app = new SolrAdminApplication(zkHost)) {
			app.execute(action, collection, query, commit, publicCollection, shadowCollection);
		}
	}

	public SolrAdminApplication(String zkHost) {
		final ZkServers zk = ZkServers.newInstance(zkHost);
		this.solrClient = new CloudSolrClient.Builder(zk.getHosts(), zk.getChroot()).build();
	}

	public SolrResponse commit(String collection) throws IOException, SolrServerException {
		return execute(Action.COMMIT, collection, null, true, null, null);
	}

	public SolrResponse execute(Action action, String collection, String query, boolean commit,
		String publicCollection, String shadowCollection)
		throws IOException, SolrServerException {
		switch (action) {

			case DELETE_BY_QUERY:
				UpdateResponse rsp = solrClient.deleteByQuery(collection, query);
				if (commit) {
					return solrClient.commit(collection);
				}
				return rsp;

			case COMMIT:
				return solrClient.commit(collection);

			case UPDATE_ALIASES:
				this.updateAliases(publicCollection, shadowCollection);
				return null;

			default:
				throw new IllegalArgumentException("action not managed: " + action);
		}
	}

	@Override
	public void close() throws IOException {
		solrClient.close();
	}

	private void updateAliases(String publicCollection, String shadowCollection)
		throws SolrServerException, IOException {

		// delete current aliases
		this.deleteAlias(ProvisionConstants.PUBLIC_ALIAS_NAME);
		this.deleteAlias(ProvisionConstants.SHADOW_ALIAS_NAME);

		// create aliases
		this.createAlias(ProvisionConstants.PUBLIC_ALIAS_NAME, publicCollection);
		this.createAlias(ProvisionConstants.SHADOW_ALIAS_NAME, shadowCollection);

	}

	public SolrResponse deleteAlias(String aliasName) throws SolrServerException, IOException {
		CollectionAdminRequest.DeleteAlias deleteAliasRequest = CollectionAdminRequest.deleteAlias(aliasName);
		return deleteAliasRequest.process(solrClient);
	}

	public SolrResponse createAlias(String aliasName, String collection) throws IOException, SolrServerException {
		CollectionAdminRequest.CreateAlias createAliasRequest = CollectionAdminRequest
			.createAlias(aliasName, collection);
		return createAliasRequest.process(solrClient);
	}

}
