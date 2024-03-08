
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.io.SAXReader;
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
	void testXmlIndexingJob_onSolr() throws Exception {

		String inputPath = "src/test/resources/eu/dnetlib/dhp/oa/provision/xml";

		Dataset<TupleWrapper> records = spark
			.read()
			.schema(Encoders.bean(TupleWrapper.class).schema())
			.json(inputPath)
			.as(Encoders.bean(TupleWrapper.class));

		long nRecord = records.count();

		new XmlIndexingJob(spark, inputPath, FORMAT, batchSize, XmlIndexingJob.OutputFormat.SOLR, true, null)
			.run(isLookupClient);

		assertEquals(0, miniCluster.getSolrClient().commit().getStatus());

		QueryResponse rsp = miniCluster.getSolrClient().query(new SolrQuery().add(CommonParams.Q, "*:*"));

		assertEquals(
			nRecord, rsp.getResults().getNumFound(),
			"the number of indexed records should be equal to the number of input records");

		rsp = miniCluster.getSolrClient().query(new SolrQuery().add(CommonParams.Q, "isgreen:true"));
		assertEquals(
			0, rsp.getResults().getNumFound(),
			"the number of indexed records having isgreen = true");

		rsp = miniCluster.getSolrClient().query(new SolrQuery().add(CommonParams.Q, "openaccesscolor:bronze"));
		assertEquals(
			0, rsp.getResults().getNumFound(),
			"the number of indexed records having openaccesscolor = bronze");

		rsp = miniCluster.getSolrClient().query(new SolrQuery().add(CommonParams.Q, "isindiamondjournal:true"));
		assertEquals(
			0, rsp.getResults().getNumFound(),
			"the number of indexed records having isindiamondjournal = true");

		rsp = miniCluster.getSolrClient().query(new SolrQuery().add(CommonParams.Q, "publiclyfunded:true"));
		assertEquals(
			0, rsp.getResults().getNumFound(),
			"the number of indexed records having publiclyfunded = true");

		rsp = miniCluster.getSolrClient().query(new SolrQuery().add(CommonParams.Q, "peerreviewed:true"));
		assertEquals(
			0, rsp.getResults().getNumFound(),
			"the number of indexed records having peerreviewed = true");

		rsp = miniCluster
			.getSolrClient()
			.query(
				new SolrQuery()
					.add(CommonParams.Q, "objidentifier:\"iddesignpres::ae77e56e84ad058d9e7f19fa2f7325db\"")
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

	}

	@Test
	void testXmlIndexingJob_saveOnHDFS() throws Exception {
		final String ID_XPATH = "//*[local-name()='header']/*[local-name()='objIdentifier']";

		String inputPath = "src/test/resources/eu/dnetlib/dhp/oa/provision/xml";
		// String inputPath = "/Users/claudio/workspace/data/index";

		Dataset<TupleWrapper> records = spark
			.read()
			.schema(Encoders.bean(TupleWrapper.class).schema())
			.json(inputPath)
			.as(Encoders.bean(TupleWrapper.class));

		records.printSchema();

		long nRecord = records.count();
		log.info("found {} records", nRecord);

		final Dataset<String> ids = records
			.map((MapFunction<TupleWrapper, String>) TupleWrapper::getXml, Encoders.STRING())
			.map(
				(MapFunction<String, String>) s -> new SAXReader().read(new StringReader(s)).valueOf(ID_XPATH),
				Encoders.STRING());

		log.info("found {} ids", ids.count());

		long xmlIdUnique = ids
			.distinct()
			.count();

		log.info("found {} unique ids", xmlIdUnique);

		assertEquals(nRecord, xmlIdUnique, "IDs should be unique among input records");

		final String outputPath = workingDir.resolve("outputPath").toAbsolutePath().toString();
		new XmlIndexingJob(spark, inputPath, FORMAT, batchSize, XmlIndexingJob.OutputFormat.HDFS, false, outputPath)
			.run(isLookupClient);

		final Dataset<SerializableSolrInputDocument> solrDocs = spark
			.read()
			.load(outputPath)
			.as(Encoders.kryo(SerializableSolrInputDocument.class));

		solrDocs.foreach(doc -> {
			assertNotNull(doc.get("__result"));
			assertNotNull(doc.get("__json"));
		});

		long docIdUnique = solrDocs.map((MapFunction<SerializableSolrInputDocument, String>) doc -> {
			final SolrInputField id = doc.getField("__indexrecordidentifier");
			return id.getFirstValue().toString();
		}, Encoders.STRING())
			.distinct()
			.count();
		assertEquals(xmlIdUnique, docIdUnique, "IDs should be unique among the output XML records");

		long jsonUnique = solrDocs
			.map(
				(MapFunction<SerializableSolrInputDocument, String>) je -> (String) je.getField("__json").getValue(),
				Encoders.STRING())
			.distinct()
			.count();

		assertEquals(jsonUnique, docIdUnique, "IDs should be unique among the output JSON records");

	}

}
