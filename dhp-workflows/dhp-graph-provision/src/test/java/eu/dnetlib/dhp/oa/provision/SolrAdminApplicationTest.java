
package eu.dnetlib.dhp.oa.provision;

import java.io.File;
import java.nio.file.Path;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

public class SolrAdminApplicationTest {

	private static final Logger log = LoggerFactory.getLogger(SolrAdminApplicationTest.class);
	public static final String DEFAULT_COLLECTION = "testCollection";
	public static final String CONFIG_NAME = "testConfig";

	private static MiniSolrCloudCluster miniCluster;
	private static CloudSolrClient cloudSolrClient;

	@TempDir
	public static Path tempDir;

	@BeforeAll
	public static void setup() throws Exception {

		// random unassigned HTTP port
		final int jettyPort = 0;

		final JettyConfig jettyConfig = JettyConfig.builder().setPort(jettyPort).build();

		// create a MiniSolrCloudCluster instance
		miniCluster = new MiniSolrCloudCluster(2, tempDir, jettyConfig);

		// Upload Solr configuration directory to ZooKeeper
		String solrZKConfigDir = "src/test/resources/eu/dnetlib/dhp/oa/provision/solr/conf/testConfig";
		File configDir = new File(solrZKConfigDir);

		miniCluster.uploadConfigSet(configDir.toPath(), CONFIG_NAME);

		// override settings in the solrconfig include
		System.setProperty("solr.tests.maxBufferedDocs", "100000");
		System.setProperty("solr.tests.maxIndexingThreads", "-1");
		System.setProperty("solr.tests.ramBufferSizeMB", "100");

		// use non-test classes so RandomizedRunner isn't necessary
		System.setProperty("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
		System.setProperty("solr.directoryFactory", "solr.RAMDirectoryFactory");

		cloudSolrClient = miniCluster.getSolrClient();
		cloudSolrClient.setRequestWriter(new RequestWriter());
		cloudSolrClient.setParser(new XMLResponseParser());
		cloudSolrClient.setDefaultCollection(DEFAULT_COLLECTION);
		cloudSolrClient.connect();

		log.info(new ConfigSetAdminRequest.List().process(cloudSolrClient).toString());
		log.info(CollectionAdminRequest.ClusterStatus.getClusterStatus().process(cloudSolrClient).toString());

		createCollection(cloudSolrClient, DEFAULT_COLLECTION, 2, 1, CONFIG_NAME);
	}

	@AfterAll
	public static void shutDown() throws Exception {
		miniCluster.shutdown();
	}

	protected static NamedList<Object> createCollection(CloudSolrClient client, String name, int numShards,
		int replicationFactor, String configName) throws Exception {
		ModifiableSolrParams modParams = new ModifiableSolrParams();
		modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
		modParams.set("name", name);
		modParams.set("numShards", numShards);
		modParams.set("replicationFactor", replicationFactor);
		modParams.set("collection.configName", configName);
		QueryRequest request = new QueryRequest(modParams);
		request.setPath("/admin/collections");
		return client.request(request);
	}

	@Test
	public void testPing() throws Exception {
		SolrPingResponse pingResponse = cloudSolrClient.ping();
		log.info("pingResponse: '{}'", pingResponse.getStatus());
		Assert.assertTrue(pingResponse.getStatus() == 0);
	}

	@Test
	public void testAdminApplication_DELETE() throws Exception {

		SolrAdminApplication admin = new SolrAdminApplication(miniCluster.getSolrClient().getZkHost());

		UpdateResponse rsp = (UpdateResponse) admin
			.execute(SolrAdminApplication.Action.DELETE_BY_QUERY, DEFAULT_COLLECTION, "*:*", false);

		Assertions.assertTrue(rsp.getStatus() == 0);
	}

	@Test
	public void testAdminApplication_COMMIT() throws Exception {

		SolrAdminApplication admin = new SolrAdminApplication(miniCluster.getSolrClient().getZkHost());

		UpdateResponse rsp = (UpdateResponse) admin.commit(DEFAULT_COLLECTION);

		Assertions.assertTrue(rsp.getStatus() == 0);
	}

}
