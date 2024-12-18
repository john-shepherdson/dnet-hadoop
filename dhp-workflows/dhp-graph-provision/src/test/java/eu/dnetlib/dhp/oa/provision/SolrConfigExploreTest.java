
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class SolrConfigExploreTest {

	protected static final Logger log = LoggerFactory.getLogger(SolrConfigExploreTest.class);

	protected static final String SHADOW_FORMAT = "c1";
	protected static final String SHADOW_COLLECTION = SHADOW_FORMAT + "-index-openaire";
	protected static final String PUBLIC_FORMAT = "c2";
	protected static final String PUBLIC_COLLECTION = PUBLIC_FORMAT + "-index-openaire";

	protected static final String CONFIG_NAME = "testConfig";

	protected static SolrAdminApplication admin;

	protected static SparkSession spark;

	private static final Integer batchSize = 100;

	@Mock
	private ISLookUpService isLookUpService;

	@Mock
	private ISLookupClient isLookupClient;

	@TempDir
	public static Path workingDir;

	protected static MiniSolrCloudCluster miniCluster;

	@BeforeEach
	public void prepareMocks() throws ISLookUpException, IOException {
		isLookupClient.setIsLookup(isLookUpService);

		int solrPort = URI.create("http://" + miniCluster.getZkClient().getZkServerAddress()).getPort();

		Mockito.when(isLookupClient.getZkHost()).thenReturn(String.format("127.0.0.1:%s/solr", solrPort));
		Mockito
			.when(isLookupClient.getLayoutSource(Mockito.anyString()))
			.thenReturn(IOUtils.toString(getClass().getResourceAsStream("fields.xml")));
		Mockito
			.when(isLookupClient.getLayoutTransformer())
			.thenReturn(IOUtils.toString(getClass().getResourceAsStream("layoutToRecordTransformer.xsl")));
	}

	@BeforeAll
	public static void setup() throws Exception {

		SparkConf conf = new SparkConf();
		conf.setAppName(XmlIndexingJobTest.class.getSimpleName());
		conf.registerKryoClasses(new Class[] {
			SerializableSolrInputDocument.class
		});

		conf.setMaster("local[1]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.resolve("spark").toString());

		spark = SparkSession
			.builder()
			.appName(SolrConfigExploreTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();

		// random unassigned HTTP port
		final int jettyPort = 0;
		final JettyConfig jettyConfig = JettyConfig.builder().setPort(jettyPort).build();

		log.info(String.format("working directory: %s", workingDir.toString()));
		System.setProperty("solr.log.dir", workingDir.resolve("logs").toString());

		// create a MiniSolrCloudCluster instance
		miniCluster = new MiniSolrCloudCluster(2, workingDir.resolve("solr"), jettyConfig);

		// Upload Solr configuration directory to ZooKeeper
		String solrZKConfigDir = "src/test/resources/eu/dnetlib/dhp/oa/provision/solr/conf/exploreTestConfig";
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
	public static void tearDown() throws Exception {
		spark.stop();
		miniCluster.shutdown();
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	@Test
	public void testSolrConfig() throws Exception {

		String inputPath = "src/test/resources/eu/dnetlib/dhp/oa/provision/xml";

		new XmlIndexingJob(spark, inputPath, SHADOW_FORMAT, ProvisionConstants.SHADOW_ALIAS_NAME, batchSize)
			.run(isLookupClient);
		Assertions
			.assertEquals(0, miniCluster.getSolrClient().commit(ProvisionConstants.SHADOW_ALIAS_NAME).getStatus());

		String[] queryStrings = {
			"cancer",
			"graph",
			"graphs"
		};

		for (String q : queryStrings) {
			SolrQuery query = new SolrQuery();
			query.setRequestHandler("/exploreSearch");
			query.add(CommonParams.Q, q);
			query.set("debugQuery", "on");

			log.info("Submit query to Solr with params: {}", query);
			QueryResponse rsp = miniCluster.getSolrClient().query(ProvisionConstants.SHADOW_ALIAS_NAME, query);
//            System.out.println(rsp.getHighlighting());
//            System.out.println(rsp.getExplainMap());

			for (SolrDocument doc : rsp.getResults()) {
				log
					.info(
						doc.get("score") + "\t" +
							doc.get("__indexrecordidentifier") + "\t" +
							doc.get("resultidentifier") + "\t" +
							doc.get("resultauthor") + "\t" +
							doc.get("resultacceptanceyear") + "\t" +
							doc.get("resultsubject") + "\t" +
							doc.get("resulttitle") + "\t" +
							doc.get("relprojectname") + "\t" +
							doc.get("resultdescription") + "\t" +
							doc.get("__all") + "\t");
			}
		}
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
