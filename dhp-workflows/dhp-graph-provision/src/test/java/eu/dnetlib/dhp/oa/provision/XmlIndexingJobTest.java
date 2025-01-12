
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class XmlIndexingJobTest extends SolrTest {

	protected static SparkSession spark;

	private static final Integer batchSize = 100;

	@Mock
	private ISLookUpService isLookUpService;

	@Mock
	private ISLookupClient isLookupClient;

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
	public static void before() {

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
			.appName(XmlIndexingJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void tearDown() {
		spark.stop();
	}

	@Test
	void testXmlIndexingJob_onSolr() throws Exception {

		String inputPath = "src/test/resources/eu/dnetlib/dhp/oa/provision/xml";

		Dataset<TupleWrapper> records = spark
			.read()
			.schema(Encoders.bean(TupleWrapper.class).schema())
			.json(inputPath)
			.as(Encoders.bean(TupleWrapper.class));

		long nRecord = records.count();

		new XmlIndexingJob(spark, inputPath, SHADOW_FORMAT, ProvisionConstants.SHADOW_ALIAS_NAME, batchSize)
			.run(isLookupClient);

		assertEquals(0, miniCluster.getSolrClient().commit(SHADOW_COLLECTION).getStatus());

		QueryResponse rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.SHADOW_ALIAS_NAME,
				new SolrQuery().add(CommonParams.Q, "*:*"));

		assertEquals(
			nRecord, rsp.getResults().getNumFound(),
			"the number of indexed records should be equal to the number of input records");

		rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.SHADOW_ALIAS_NAME,
				new SolrQuery().add(CommonParams.Q, "isgreen:true"));
		assertEquals(
			4, rsp.getResults().getNumFound(),
			"the number of indexed records having isgreen = true");

		rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.SHADOW_ALIAS_NAME,
				new SolrQuery().add(CommonParams.Q, "openaccesscolor:bronze"));
		assertEquals(
			2, rsp.getResults().getNumFound(),
			"the number of indexed records having openaccesscolor = bronze");

		rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.SHADOW_ALIAS_NAME,
				new SolrQuery().add(CommonParams.Q, "isindiamondjournal:true"));
		assertEquals(
			0, rsp.getResults().getNumFound(),
			"the number of indexed records having isindiamondjournal = true");

		rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.SHADOW_ALIAS_NAME,
				new SolrQuery().add(CommonParams.Q, "publiclyfunded:true"));
		assertEquals(
			0, rsp.getResults().getNumFound(),
			"the number of indexed records having publiclyfunded = true");

		rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.SHADOW_ALIAS_NAME,
				new SolrQuery().add(CommonParams.Q, "peerreviewed:true"));
		assertEquals(
			35, rsp.getResults().getNumFound(),
			"the number of indexed records having peerreviewed = true");

		rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.SHADOW_ALIAS_NAME,
				new SolrQuery()
					.add(CommonParams.Q, "objidentifier:\"57a035e5b1ae::236d6d8c1e03368b5ae72acfeeb11bbc\"")
					.add(CommonParams.FL, "__json"));
		assertEquals(
			1, rsp.getResults().getNumFound(),
			"the number of indexed records having the given identifier");
		Optional<Object> json = rsp
			.getResults()
			.stream()
			.map(d -> d.getFieldValues("__json"))
			.flatMap(d -> d.stream())
			.findFirst();

		assertTrue(json.isPresent());

		log.info((String) json.get());

		admin
			.execute(
				SolrAdminApplication.Action.UPDATE_ALIASES, null, false,
				SHADOW_COLLECTION, PUBLIC_COLLECTION);

		rsp = miniCluster
			.getSolrClient()
			.query(
				ProvisionConstants.PUBLIC_ALIAS_NAME,
				new SolrQuery()
					.add(CommonParams.Q, "objidentifier:\"57a035e5b1ae::236d6d8c1e03368b5ae72acfeeb11bbc\"")
					.add(CommonParams.FL, "__json"));

		assertEquals(
			1, rsp.getResults().getNumFound(),
			"the number of indexed records having the given identifier, found in the public collection");
	}

}
