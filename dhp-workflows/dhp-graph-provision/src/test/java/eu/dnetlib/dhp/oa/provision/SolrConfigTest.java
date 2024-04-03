
package eu.dnetlib.dhp.oa.provision;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class SolrConfigTest extends SolrTest {

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

		Mockito
			.when(isLookupClient.getDsId(Mockito.anyString()))
			.thenReturn("313f0381-23b6-466f-a0b8-c72a9679ac4b_SW5kZXhEU1Jlc291cmNlcy9JbmRleERTUmVzb3VyY2VUeXBl");
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
	public void testSolrConfig() throws Exception {

		String inputPath = "src/test/resources/eu/dnetlib/dhp/oa/provision/xml";

		new XmlIndexingJob(spark, inputPath, FORMAT, batchSize)
			.run(isLookupClient);
		Assertions.assertEquals(0, miniCluster.getSolrClient().commit().getStatus());

		String[] queryStrings = {
			"cancer",
			"graph",
			"graphs"
		};

		for (String q : queryStrings) {
			SolrQuery query = new SolrQuery();
			query.add(CommonParams.Q, q);

			log.info("Submit query to Solr with params: {}", query.toString());
			QueryResponse rsp = miniCluster.getSolrClient().query(query);

			for (SolrDocument doc : rsp.getResults()) {
				System.out
					.println(
						doc.get("__indexrecordidentifier") + "\t" +
							doc.get("__result") + "\t");
			}
		}
	}
}
