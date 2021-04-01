
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.AuthorSummary;
import eu.dnetlib.dhp.schema.orcid.Work;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import eu.dnetlib.doiboost.orcidnodoi.xml.XMLRecordParserNoDoi;
import scala.Tuple2;

public class SparkUpdateOrcidDatasets {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.setSerializationInclusion(JsonInclude.Include.NON_NULL);

	public static void main(String[] args) throws IOException, Exception {
		Logger logger = LoggerFactory.getLogger(SparkUpdateOrcidDatasets.class);

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkUpdateOrcidDatasets.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		final String workingPath = parser.get("workingPath");
//		final String outputPath = parser.get("outputPath");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				LongAccumulator oldAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("old_authors_found");
				LongAccumulator updatedAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("updated_authors_found");
				LongAccumulator newAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("new_authors_found");
				LongAccumulator errorCodeAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_code_authors_found");
				LongAccumulator errorLoadingAuthorsJsonFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_loading_authors_json_found");
				LongAccumulator errorParsingAuthorsXMLFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_parsing_authors_xml_found");

				LongAccumulator oldWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("old_works_found");
				LongAccumulator updatedWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("updated_works_found");
				LongAccumulator newWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("new_works_found");
				LongAccumulator errorCodeWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_code_works_found");
				LongAccumulator errorLoadingWorksJsonFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_loading_works_json_found");
				LongAccumulator errorParsingWorksXMLFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_parsing_works_xml_found");

//				JavaPairRDD<Text, Text> xmlSummariesRDD = sc
//					.sequenceFile(workingPath.concat("xml/authors/xml_authors.seq"), Text.class, Text.class);
//				xmlSummariesRDD
//					.map(seq -> {
//						AuthorSummary authorSummary = XMLRecordParser
//							.VTDParseAuthorSummary(seq._2().toString().getBytes());
//						authorSummary
//							.setBase64CompressData(ArgumentApplicationParser.compressArgument(seq._2().toString()));
//						return authorSummary;
//					})
//					.filter(authorSummary -> authorSummary != null)
//					.map(authorSummary -> JsonWriter.create(authorSummary))
//					.saveAsTextFile(workingPath.concat("orcid_dataset/authors"), GzipCodec.class);
//
//				JavaPairRDD<Text, Text> xmlWorksRDD = sc
//					.sequenceFile(workingPath.concat("xml/works/*"), Text.class, Text.class);
//
//				xmlWorksRDD
//					.map(seq -> {
//						WorkDetail workDetail = XMLRecordParserNoDoi.VTDParseWorkData(seq._2().toString().getBytes());
//						Work work = new Work();
//						work.setWorkDetail(workDetail);
//						work.setBase64CompressData(ArgumentApplicationParser.compressArgument(seq._2().toString()));
//						return work;
//					})
//					.filter(work -> work != null)
//					.map(work -> JsonWriter.create(work))
//					.saveAsTextFile(workingPath.concat("orcid_dataset/works"), GzipCodec.class);

//				Function<Tuple2<Text, Text>, AuthorSummary> retrieveAuthorSummaryFunction = data -> {
//					AuthorSummary authorSummary = new AuthorSummary();
//					String orcidId = data._1().toString();
//					String jsonData = data._2().toString();
//					JsonElement jElement = new JsonParser().parse(jsonData);
//					String statusCode = getJsonValue(jElement, "statusCode");
//					String downloadDate = getJsonValue(jElement, "lastModifiedDate");
//					if (statusCode.equals("200")) {
//						String compressedData = getJsonValue(jElement, "compressedData");
//						if (StringUtils.isEmpty(compressedData)) {
//							errorLoadingAuthorsJsonFoundAcc.add(1);
//						} else {
//							String xmlAuthor = ArgumentApplicationParser.decompressValue(compressedData);
//							try {
//								authorSummary = XMLRecordParser
//									.VTDParseAuthorSummary(xmlAuthor.getBytes());
//								authorSummary.setStatusCode(statusCode);
//								authorSummary.setDownloadDate("2020-11-18 00:00:05.644768");
//								authorSummary.setBase64CompressData(compressedData);
//								return authorSummary;
//							} catch (Exception e) {
//								logger.error("parsing xml " + orcidId + " [" + jsonData + "]", e);
//								errorParsingAuthorsXMLFoundAcc.add(1);
//							}
//						}
//					} else {
//						authorSummary.setStatusCode(statusCode);
//						authorSummary.setDownloadDate("2020-11-18 00:00:05.644768");
//						errorCodeAuthorsFoundAcc.add(1);
//					}
//					return authorSummary;
//				};
//
//				Dataset<AuthorSummary> downloadedAuthorSummaryDS = spark
//					.createDataset(
//						sc
//							.sequenceFile(workingPath + "downloads/updated_authors/*", Text.class, Text.class)
//							.map(retrieveAuthorSummaryFunction)
//							.rdd(),
//						Encoders.bean(AuthorSummary.class));
//				Dataset<AuthorSummary> currentAuthorSummaryDS = spark
//					.createDataset(
//						sc
//							.textFile(workingPath.concat("orcid_dataset/authors/*"))
//							.map(item -> OBJECT_MAPPER.readValue(item, AuthorSummary.class))
//							.rdd(),
//						Encoders.bean(AuthorSummary.class));
//				currentAuthorSummaryDS
//					.joinWith(
//						downloadedAuthorSummaryDS,
//						currentAuthorSummaryDS
//							.col("authorData.oid")
//							.equalTo(downloadedAuthorSummaryDS.col("authorData.oid")),
//						"full_outer")
//					.map(value -> {
//						Optional<AuthorSummary> opCurrent = Optional.ofNullable(value._1());
//						Optional<AuthorSummary> opDownloaded = Optional.ofNullable(value._2());
//						if (!opCurrent.isPresent()) {
//							newAuthorsFoundAcc.add(1);
//							return opDownloaded.get();
//						}
//						if (!opDownloaded.isPresent()) {
//							oldAuthorsFoundAcc.add(1);
//							return opCurrent.get();
//						}
//						if (opCurrent.isPresent() && opDownloaded.isPresent()) {
//							updatedAuthorsFoundAcc.add(1);
//							return opDownloaded.get();
//						}
//						return null;
//					},
//						Encoders.bean(AuthorSummary.class))
//					.filter(Objects::nonNull)
//					.toJavaRDD()
//					.map(authorSummary -> OBJECT_MAPPER.writeValueAsString(authorSummary))
//					.saveAsTextFile(workingPath.concat("orcid_dataset/new_authors"), GzipCodec.class);
//
//				logger.info("oldAuthorsFoundAcc: " + oldAuthorsFoundAcc.value().toString());
//				logger.info("newAuthorsFoundAcc: " + newAuthorsFoundAcc.value().toString());
//				logger.info("updatedAuthorsFoundAcc: " + updatedAuthorsFoundAcc.value().toString());
//				logger.info("errorCodeFoundAcc: " + errorCodeAuthorsFoundAcc.value().toString());
//				logger.info("errorLoadingJsonFoundAcc: " + errorLoadingAuthorsJsonFoundAcc.value().toString());
//				logger.info("errorParsingXMLFoundAcc: " + errorParsingAuthorsXMLFoundAcc.value().toString());

				Function<String, Work> retrieveWorkFunction = jsonData -> {
					Work work = new Work();
					JsonElement jElement = new JsonParser().parse(jsonData);
					String statusCode = getJsonValue(jElement, "statusCode");
					work.setStatusCode(statusCode);
					String downloadDate = getJsonValue(jElement, "lastModifiedDate");
					work.setDownloadDate("2020-11-18 00:00:05.644768");
					if (statusCode.equals("200")) {
						String compressedData = getJsonValue(jElement, "compressedData");
						if (StringUtils.isEmpty(compressedData)) {
							errorLoadingWorksJsonFoundAcc.add(1);
						} else {
							String xmlWork = ArgumentApplicationParser.decompressValue(compressedData);
							try {
								WorkDetail workDetail = XMLRecordParserNoDoi
									.VTDParseWorkData(xmlWork.getBytes());
								work.setWorkDetail(workDetail);
								work.setBase64CompressData(compressedData);
								return work;
							} catch (Exception e) {
								logger.error("parsing xml [" + jsonData + "]", e);
								errorParsingWorksXMLFoundAcc.add(1);
							}
						}
					} else {
						errorCodeWorksFoundAcc.add(1);
					}
					return work;
				};

				Dataset<Work> downloadedWorksDS = spark
					.createDataset(
						sc
							.textFile(workingPath + "downloads/updated_works/*")
							.map(s -> {
								return s.substring(21, s.length() - 1);
							})
							.map(retrieveWorkFunction)
							.rdd(),
						Encoders.bean(Work.class));
				Dataset<Work> currentWorksDS = spark
					.createDataset(
						sc
							.textFile(workingPath.concat("orcid_dataset/works/*"))
							.map(item -> OBJECT_MAPPER.readValue(item, Work.class))
							.rdd(),
						Encoders.bean(Work.class));
				currentWorksDS
					.joinWith(
						downloadedWorksDS,
						currentWorksDS
							.col("workDetail.id")
							.equalTo(downloadedWorksDS.col("workDetail.id"))
							.and(
								currentWorksDS
									.col("workDetail.oid")
									.equalTo(downloadedWorksDS.col("workDetail.oid"))),
						"full_outer")
					.map(value -> {
						Optional<Work> opCurrent = Optional.ofNullable(value._1());
						Optional<Work> opDownloaded = Optional.ofNullable(value._2());
						if (!opCurrent.isPresent()) {
							newWorksFoundAcc.add(1);
							return opDownloaded.get();
						}
						if (!opDownloaded.isPresent()) {
							oldWorksFoundAcc.add(1);
							return opCurrent.get();
						}
						if (opCurrent.isPresent() && opDownloaded.isPresent()) {
							updatedWorksFoundAcc.add(1);
							return opDownloaded.get();
						}
						return null;
					},
						Encoders.bean(Work.class))
					.filter(Objects::nonNull)
					.toJavaRDD()
					.map(work -> OBJECT_MAPPER.writeValueAsString(work))
					.saveAsTextFile(workingPath.concat("orcid_dataset/new_works"), GzipCodec.class);

				logger.info("oldWorksFoundAcc: " + oldWorksFoundAcc.value().toString());
				logger.info("newWorksFoundAcc: " + newWorksFoundAcc.value().toString());
				logger.info("updatedWorksFoundAcc: " + updatedWorksFoundAcc.value().toString());
				logger.info("errorCodeWorksFoundAcc: " + errorCodeWorksFoundAcc.value().toString());
				logger.info("errorLoadingJsonWorksFoundAcc: " + errorLoadingWorksJsonFoundAcc.value().toString());
				logger.info("errorParsingXMLWorksFoundAcc: " + errorParsingWorksXMLFoundAcc.value().toString());

			});
	}

	private static String getJsonValue(JsonElement jElement, String property) {
		if (jElement.getAsJsonObject().has(property)) {
			JsonElement name = null;
			name = jElement.getAsJsonObject().get(property);
			if (name != null && !name.isJsonNull()) {
				return name.getAsString();
			}
		}
		return "";
	}
}
