
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.toSeq;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.provision.model.*;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.solr.SolrRecord;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

/**
 * XmlConverterJob converts the JoinedEntities as XML records
 */
public class PayloadConverterJob {

	private static final Logger log = LoggerFactory.getLogger(PayloadConverterJob.class);

	public static final String schemaLocation = "https://www.openaire.eu/schema/1.0/oaf-1.0.xsd";

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PayloadConverterJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_payload_converter.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final Boolean validateXML = Optional
			.ofNullable(parser.get("validateXML"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);
		log.info("validateXML: {}", validateXML);

		final String contextApiBaseUrl = parser.get("contextApiBaseUrl");
		log.info("contextApiBaseUrl: {}", contextApiBaseUrl);

		String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		ISLookUpService isLookup = ISLookupClientFactory.getLookUpService(isLookupUrl);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			removeOutputDir(spark, outputPath);
			createPayloads(
				spark, inputPath, outputPath, ContextMapper.fromAPI(contextApiBaseUrl),
				VocabularyGroup.loadVocsFromIS(isLookup), validateXML);
		});
	}

	private static void createPayloads(
		final SparkSession spark,
		final String inputPath,
		final String outputPath,
		final ContextMapper contextMapper,
		final VocabularyGroup vocabularies,
		final Boolean validateXML) {

		final XmlRecordFactory recordFactory = new XmlRecordFactory(
			contextMapper,
			false,
			schemaLocation);

		final List<String> paths = HdfsSupport
			.listFiles(inputPath, spark.sparkContext().hadoopConfiguration());

		log.info("Found paths: {}", String.join(",", paths));

		final ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

		createPayloads(
			spark, inputPath + "/publication", JoinedPublication.class, recordFactory, validateXML, contextMapper,
			vocabularies)
				.union(
					createPayloads(
						spark, inputPath + "/dataset", JoinedDataset.class, recordFactory, validateXML, contextMapper,
						vocabularies))
				.union(
					createPayloads(
						spark, inputPath + "/otherresearchproduct", JoinedOrp.class, recordFactory, validateXML,
						contextMapper, vocabularies))
				.union(
					createPayloads(
						spark, inputPath + "/Software", JoinedSoftware.class, recordFactory, validateXML, contextMapper,
						vocabularies))
				.union(
					createPayloads(
						spark, inputPath + "/datasource", JoinedDatasource.class, recordFactory, validateXML,
						contextMapper, vocabularies))
				.union(
					createPayloads(
						spark, inputPath + "/project", JoinedProject.class, recordFactory, validateXML, contextMapper,
						vocabularies))
				.union(
					createPayloads(
						spark, inputPath + "/organization", JoinedOrganization.class, recordFactory, validateXML,
						contextMapper, vocabularies))
				// .union(createPayloads(spark, inputPath + "/person", JoinedPerson.class, recordFactory, validateXML,
				// contextMapper, vocabularies))
				.map(
					(MapFunction<Tuple2<String, SolrRecord>, TupleWrapper>) t -> new TupleWrapper(
						t._1(), mapper.writeValueAsString(t._2())),
					Encoders.bean(TupleWrapper.class))
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression", "gzip")
				.json(outputPath);
	}

	private static <T extends JoinedEntity> Dataset<Tuple2<String, SolrRecord>> createPayloads(SparkSession spark,
		String path, Class<T> clazz, XmlRecordFactory recordFactory, boolean validateXML, ContextMapper contextMapper,
		VocabularyGroup vocabularies) {
		return spark
			.read()
			.json(path)
			.as(Encoders.bean(clazz))
			.where("entity.dataInfo.deletedbyinference == false")
			.map(
				(MapFunction<T, Tuple2<String, SolrRecord>>) je -> new Tuple2<>(
					recordFactory.build(je, validateXML),
					ProvisionModelSupport.transform(je, contextMapper, vocabularies)),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(SolrRecord.class)));
	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

}
