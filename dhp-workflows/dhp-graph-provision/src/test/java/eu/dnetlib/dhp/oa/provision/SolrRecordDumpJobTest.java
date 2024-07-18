/*
 * Copyright (c) 2024.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputField;
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
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
class SolrRecordDumpJobTest {

	protected static final Logger log = LoggerFactory.getLogger(SolrRecordDumpJobTest.class);

	protected static SparkSession spark;

	protected static final String FORMAT = "test";

	@Mock
	private ISLookUpService isLookUpService;

	@Mock
	private ISLookupClient isLookupClient;

	@TempDir
	public static Path workingDir;

	@BeforeAll
	public static void before() {

		SparkConf conf = new SparkConf();
		conf.setAppName(SolrRecordDumpJobTest.class.getSimpleName());

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
			.appName(SolrRecordDumpJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void shutDown() throws Exception {
		spark.stop();
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	@BeforeEach
	public void prepareMocks() throws ISLookUpException, IOException {
		isLookupClient.setIsLookup(isLookUpService);

		Mockito
			.when(isLookupClient.getLayoutSource(Mockito.anyString()))
			.thenReturn(IOUtils.toString(getClass().getResourceAsStream("fields.xml")));
		Mockito
			.when(isLookupClient.getLayoutTransformer())
			.thenReturn(IOUtils.toString(getClass().getResourceAsStream("layoutToRecordTransformer.xsl")));
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
		new SolrRecordDumpJob(spark, inputPath, FORMAT, outputPath).run(isLookupClient);

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
