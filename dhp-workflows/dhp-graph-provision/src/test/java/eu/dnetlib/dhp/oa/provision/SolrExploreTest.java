
package eu.dnetlib.dhp.oa.provision;

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
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

public abstract class SolrExploreTest {

	protected static final Logger log = LoggerFactory.getLogger(SolrTest.class);

	protected static final String FORMAT = "test";
	protected static final String DEFAULT_COLLECTION = FORMAT + "-index-openaire";
	protected static final String CONFIG_NAME = "testConfig";

	protected static MiniSolrCloudCluster miniCluster;

	@TempDir
	public static Path workingDir;

	@AfterAll
	public static void shutDown() throws Exception {
		miniCluster.shutdown();
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	protected static NamedList<Object> createCollection(CloudSolrClient client, String name, int numShards,
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
