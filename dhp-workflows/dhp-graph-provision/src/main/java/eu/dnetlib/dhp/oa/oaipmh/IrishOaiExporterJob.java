
package eu.dnetlib.dhp.oa.oaipmh;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;

public class IrishOaiExporterJob {

	private static final Logger log = LoggerFactory.getLogger(IrishOaiExporterJob.class);

	protected static final int NUM_CONNECTIONS = 20;

	public static final String TMP_OAI_TABLE = "temp_oai_data";

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					IrishOaiExporterJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/oaipmh/input_params_irish_oai_exporter.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		final String dbUrl = parser.get("dbUrl");
		final String dbUser = parser.get("dbUser");
		final String dbPwd = parser.get("dbPwd");
		final int numConnections = Optional
			.ofNullable(parser.get("numConnections"))
			.map(Integer::valueOf)
			.orElse(NUM_CONNECTIONS);

		log.info("inputPath:     '{}'", inputPath);
		log.info("dbUrl:         '{}'", dbUrl);
		log.info("dbUser:        '{}'", dbUser);
		log.info("dbPwd:         '{}'", "xxx");
		log.info("numPartitions: '{}'", numConnections);

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);

		final SparkConf conf = new SparkConf();
		conf.registerKryoClasses(new Class[] {
			SerializableSolrInputDocument.class
		});

		final Encoder<TupleWrapper> encoderTuple = Encoders.bean(TupleWrapper.class);
		final Encoder<OaiRecordWrapper> encoderOaiRecord = Encoders.bean(OaiRecordWrapper.class);

		final String date = LocalDateTime.now().toString();

		log.info("Creating temporary table...");
		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			final Dataset<OaiRecordWrapper> docs = spark
				.read()
				.schema(encoderTuple.schema())
				.json(inputPath)
				.as(encoderTuple)
				.map((MapFunction<TupleWrapper, String>) TupleWrapper::getXml, Encoders.STRING())
				.map((MapFunction<String, OaiRecordWrapper>) r -> asIrishOaiResult(r, date), encoderOaiRecord)
				.filter((FilterFunction<OaiRecordWrapper>) obj -> (obj != null) && StringUtils.isNotBlank(obj.getId()));

			docs
				.repartition(numConnections)
				.write()
				.mode(SaveMode.Overwrite)
				.jdbc(dbUrl, TMP_OAI_TABLE, connectionProperties);

		});
		log.info("Temporary table created.");

		log.info("Updating OAI records...");
		try (final Connection con = DriverManager.getConnection(dbUrl, dbUser, dbPwd)) {
			try (final Statement st = con.createStatement()) {
				final String query = IOUtils
					.toString(IrishOaiExporterJob.class.getResourceAsStream("oai-finalize.sql"));
				st.execute(query);
			}
		}
		log.info("DONE.");
	}

	protected static OaiRecordWrapper asIrishOaiResult(final String xml, final String date) {
		try {
			final Document doc = DocumentHelper.parseText(xml);
			final OaiRecordWrapper r = new OaiRecordWrapper();

			if (isValid(doc)) {
				r.setId(doc.valueOf("//*[local-name()='objIdentifier']").trim());
				r.setBody(gzip(doc.selectSingleNode("//*[local-name()='entity']").asXML()));
				r.setDate(date);
				r.setSets(new ArrayList<>());
			}
			return r;
		} catch (final Exception e) {
			log.error("Error parsing record: " + xml, e);
			throw new RuntimeException("Error parsing record: " + xml, e);
		}
	}

	protected static boolean isValid(final Document doc) {

		final Node n = doc.selectSingleNode("//*[local-name()='entity']/*[local-name()='result']");

		if (n != null) {

			for (final Object o : n.selectNodes(".//*[local-name()='datainfo']/*[local-name()='deletedbyinference']")) {
				if ("true".equals(((Node) o).getText().trim())) {
					return false;
				}
			}

			// verify the main country of the result
			for (final Object o : n.selectNodes("./*[local-name()='country']")) {
				if ("IE".equals(((Node) o).valueOf("@classid").trim())) {
					return true;
				}
			}

			// verify the countries of the related organizations
			for (final Object o : n.selectNodes(".//*[local-name()='rel']")) {
				final String relType = ((Node) o).valueOf("./*[local-name() = 'to']/@type").trim();
				final String relCountry = ((Node) o).valueOf("./*[local-name() = 'country']/@classid").trim();
				if ("organization".equals(relType) && "IE".equals(relCountry)) {
					return true;
				}
			}
		}
		return false;

	}

	protected static byte[] gzip(final String str) {
		if (StringUtils.isBlank(str)) {
			return null;
		}

		try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (final GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
				IOUtils.write(str.getBytes(Charset.defaultCharset()), gzip);
			}
			return baos.toByteArray();
		} catch (final IOException e) {
			throw new RuntimeException("error in gzip", e);
		}
	}

}
