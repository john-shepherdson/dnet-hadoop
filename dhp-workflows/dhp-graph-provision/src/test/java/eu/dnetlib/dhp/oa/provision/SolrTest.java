
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.security.provider.SHA;

public abstract class SolrTest {

	protected static final Logger log = LoggerFactory.getLogger(SolrTest.class);

	protected static final String SHADOW_FORMAT = "c1";
	protected static final String SHADOW_COLLECTION = SHADOW_FORMAT + "-index-openaire";
	protected static final String PUBLIC_FORMAT = "c2";
	protected static final String PUBLIC_COLLECTION = PUBLIC_FORMAT + "-index-openaire";

	protected static final String CONFIG_NAME = "testConfig";

	protected static SolrAdminApplication admin;

	protected static MiniSolrCloudCluster miniCluster;

	@TempDir
	public static Path workingDir;

	@BeforeAll
	public static void setup() throws Exception {

		// random unassigned HTTP port
		final int jettyPort = 0;
		final JettyConfig jettyConfig = JettyConfig.builder().setPort(jettyPort).build();

		log.info(String.format("working directory: %s", workingDir.toString()));
		System.setProperty("solr.log.dir", workingDir.resolve("logs").toString());

		// create a MiniSolrCloudCluster instance
		miniCluster = new MiniSolrCloudCluster(2, workingDir.resolve("solr"), jettyConfig);

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
		System.setProperty("solr.lock.type", "single");

		log.info(new ConfigSetAdminRequest.List().process(miniCluster.getSolrClient()).toString());
		log
			.info(
				CollectionAdminRequest.ClusterStatus
					.getClusterStatus()
					.process(miniCluster.getSolrClient())
					.toString());

		NamedList<Object> res = createCollection(
			miniCluster.getSolrClient(), SHADOW_COLLECTION, 4, 2, 20, CONFIG_NAME);
		res.forEach(o -> log.info(o.toString()));

		// miniCluster.getSolrClient().setDefaultCollection(SHADOW_COLLECTION);

		res = createCollection(
			miniCluster.getSolrClient(), PUBLIC_COLLECTION, 4, 2, 20, CONFIG_NAME);
		res.forEach(o -> log.info(o.toString()));

		admin = new SolrAdminApplication(miniCluster.getZkClient().getZkServerAddress());
		CollectionAdminResponse rsp = (CollectionAdminResponse) admin
			.createAlias(ProvisionConstants.PUBLIC_ALIAS_NAME, PUBLIC_COLLECTION);
		assertEquals(0, rsp.getStatus());
		rsp = (CollectionAdminResponse) admin.createAlias(ProvisionConstants.SHADOW_ALIAS_NAME, SHADOW_COLLECTION);
		assertEquals(0, rsp.getStatus());

		log
			.info(
				CollectionAdminRequest.ClusterStatus
					.getClusterStatus()
					.process(miniCluster.getSolrClient())
					.toString());
	}

	@AfterAll
	public static void shutDown() throws Exception {
		miniCluster.shutdown();
		admin.close();
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	public static NamedList<Object> createCollection(CloudSolrClient client, String name, int numShards,
		int replicationFactor, int maxShardsPerNode, String configName) throws Exception {
		ModifiableSolrParams modParams = new ModifiableSolrParams();
		modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
		modParams.set("name", name);
		modParams.set("numShards", numShards);
		modParams.set("replicationFactor", replicationFactor);
		modParams.set("collection.configName", configName);
		modParams.set("maxShardsPerNode", maxShardsPerNode);
		QueryRequest request = new QueryRequest(modParams);
		request.setPath("/admin/collections");
		return client.request(request);
	}

}
