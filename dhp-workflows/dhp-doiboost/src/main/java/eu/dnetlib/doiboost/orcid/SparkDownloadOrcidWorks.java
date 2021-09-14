
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.model.DownloadedRecordData;
import eu.dnetlib.doiboost.orcid.util.HDFSUtil;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import scala.Tuple2;

public class SparkDownloadOrcidWorks {

	static Logger logger = LoggerFactory.getLogger(SparkDownloadOrcidWorks.class);
	public static final String LAMBDA_FILE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final DateTimeFormatter LAMBDA_FILE_DATE_FORMATTER = DateTimeFormatter
		.ofPattern(LAMBDA_FILE_DATE_FORMAT);
	public static final String ORCID_XML_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public static final DateTimeFormatter ORCID_XML_DATETIMEFORMATTER = DateTimeFormatter
		.ofPattern(ORCID_XML_DATETIME_FORMAT);

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkDownloadOrcidWorks.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
		final String workingPath = parser.get("workingPath");
		logger.info("workingPath: {}", workingPath);
		final String outputPath = parser.get("outputPath");
		final String token = parser.get("token");
		final String hdfsServerUri = parser.get("hdfsServerUri");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				final String lastUpdateValue = HDFSUtil.readFromTextFile(hdfsServerUri, workingPath, "last_update.txt");
				logger.info("lastUpdateValue: ", lastUpdateValue);

				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
				LongAccumulator updatedAuthorsAcc = spark.sparkContext().longAccumulator("updated_authors");
				LongAccumulator parsedAuthorsAcc = spark.sparkContext().longAccumulator("parsed_authors");
				LongAccumulator parsedWorksAcc = spark.sparkContext().longAccumulator("parsed_works");
				LongAccumulator modifiedWorksAcc = spark.sparkContext().longAccumulator("modified_works");
				LongAccumulator maxModifiedWorksLimitAcc = spark
					.sparkContext()
					.longAccumulator("max_modified_works_limit");
				LongAccumulator errorCodeFoundAcc = spark.sparkContext().longAccumulator("error_code_found");
				LongAccumulator errorLoadingJsonFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_loading_json_found");
				LongAccumulator errorLoadingXMLFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_loading_xml_found");
				LongAccumulator errorParsingXMLFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_parsing_xml_found");
				LongAccumulator downloadedRecordsAcc = spark.sparkContext().longAccumulator("downloaded_records");
				LongAccumulator errorHTTP403Acc = spark.sparkContext().longAccumulator("error_HTTP_403");
				LongAccumulator errorHTTP404Acc = spark.sparkContext().longAccumulator("error_HTTP_404");
				LongAccumulator errorHTTP409Acc = spark.sparkContext().longAccumulator("error_HTTP_409");
				LongAccumulator errorHTTP503Acc = spark.sparkContext().longAccumulator("error_HTTP_503");
				LongAccumulator errorHTTP525Acc = spark.sparkContext().longAccumulator("error_HTTP_525");
				LongAccumulator errorHTTPGenericAcc = spark.sparkContext().longAccumulator("error_HTTP_Generic");
				LongAccumulator unknowHostAcc = spark.sparkContext().longAccumulator("error_unknowHost");

				JavaPairRDD<Text, Text> updatedAuthorsRDD = sc
					.sequenceFile(workingPath + "downloads/updated_authors/*", Text.class, Text.class);
				updatedAuthorsAcc.setValue(updatedAuthorsRDD.count());

				FlatMapFunction<Tuple2<Text, Text>, String> retrieveWorkUrlFunction = data -> {
					String orcidId = data._1().toString();
					String jsonData = data._2().toString();
					List<String> workIds = new ArrayList<>();
					Map<String, String> workIdLastModifiedDate = new HashMap<>();
					JsonElement jElement = new JsonParser().parse(jsonData);
					String statusCode = getJsonValue(jElement, "statusCode");
					if (statusCode.equals("200")) {
						String compressedData = getJsonValue(jElement, "compressedData");
						if (StringUtils.isEmpty(compressedData)) {
							errorLoadingJsonFoundAcc.add(1);
						} else {
							String authorSummary = ArgumentApplicationParser.decompressValue(compressedData);
							if (StringUtils.isEmpty(authorSummary)) {
								errorLoadingXMLFoundAcc.add(1);
							} else {
								try {
									workIdLastModifiedDate = XMLRecordParser
										.retrieveWorkIdLastModifiedDate(authorSummary.getBytes());
								} catch (Exception e) {
									logger.error("parsing " + orcidId + " [" + jsonData + "]", e);
									errorParsingXMLFoundAcc.add(1);
								}
							}
						}
					} else {
						errorCodeFoundAcc.add(1);
					}
					parsedAuthorsAcc.add(1);
					workIdLastModifiedDate.forEach((k, v) -> {
						parsedWorksAcc.add(1);
						if (isModified(orcidId, v, lastUpdateValue)) {
							modifiedWorksAcc.add(1);
							workIds.add(orcidId.concat("/work/").concat(k));
						}
					});
					if (workIdLastModifiedDate.size() > 50) {
						maxModifiedWorksLimitAcc.add(1);
					}
					return workIds.iterator();
				};

				Function<String, Tuple2<String, String>> downloadWorkFunction = data -> {
					String relativeWorkUrl = data;
					String orcidId = relativeWorkUrl.split("/")[0];
					final DownloadedRecordData downloaded = new DownloadedRecordData();
					downloaded.setOrcidId(orcidId);
					downloaded.setLastModifiedDate(lastUpdateValue);
					CloseableHttpClient client = HttpClients.createDefault();
					HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + relativeWorkUrl);
					httpGet.addHeader("Accept", "application/vnd.orcid+xml");
					httpGet.addHeader("Authorization", String.format("Bearer %s", token));
					long startReq = System.currentTimeMillis();
					CloseableHttpResponse response = null;
					try {
						response = client.execute(httpGet);
					} catch (UnknownHostException u) {
						downloaded.setStatusCode(-1);
						unknowHostAcc.add(1);
						if (client != null) {
							client.close();
						}
						return downloaded.toTuple2();
					}
					long endReq = System.currentTimeMillis();
					long reqTime = endReq - startReq;
					if (reqTime < 1000) {
						Thread.sleep(1000 - reqTime);
					}
					int statusCode = response.getStatusLine().getStatusCode();
					downloaded.setStatusCode(statusCode);
					if (statusCode != 200) {
						switch (statusCode) {
							case 403:
								errorHTTP403Acc.add(1);
								break;
							case 404:
								errorHTTP404Acc.add(1);
								break;
							case 409:
								errorHTTP409Acc.add(1);
								break;
							case 503:
								errorHTTP503Acc.add(1);
								break;
							case 525:
								errorHTTP525Acc.add(1);
								break;
							default:
								errorHTTPGenericAcc.add(1);
								logger
									.info(
										"Downloading {} status code: {}", orcidId,
										response.getStatusLine().getStatusCode());
						}
						return downloaded.toTuple2();
					}
					downloadedRecordsAcc.add(1);
					downloaded
						.setCompressedData(
							ArgumentApplicationParser
								.compressArgument(IOUtils.toString(response.getEntity().getContent())));
					client.close();
					return downloaded.toTuple2();
				};

				updatedAuthorsRDD
					.flatMap(retrieveWorkUrlFunction)
					.repartition(100)
					.map(downloadWorkFunction)
					.mapToPair(t -> new Tuple2<>(new Text(t._1()), new Text(t._2())))
					.saveAsTextFile(workingPath.concat(outputPath), GzipCodec.class);

				logger.info("updatedAuthorsAcc: {}", updatedAuthorsAcc.value());
				logger.info("parsedAuthorsAcc: {}", parsedAuthorsAcc.value());
				logger.info("parsedWorksAcc: {}", parsedWorksAcc.value());
				logger.info("modifiedWorksAcc: {}", modifiedWorksAcc.value());
				logger.info("maxModifiedWorksLimitAcc: {}", maxModifiedWorksLimitAcc.value());
				logger.info("errorCodeFoundAcc: {}", errorCodeFoundAcc.value());
				logger.info("errorLoadingJsonFoundAcc: {}", errorLoadingJsonFoundAcc.value());
				logger.info("errorLoadingXMLFoundAcc: {}", errorLoadingXMLFoundAcc.value());
				logger.info("errorParsingXMLFoundAcc: {}", errorParsingXMLFoundAcc.value());
				logger.info("downloadedRecordsAcc: {}", downloadedRecordsAcc.value());
				logger.info("errorHTTP403Acc: {}", errorHTTP403Acc.value());
				logger.info("errorHTTP409Acc: {}", errorHTTP409Acc.value());
				logger.info("errorHTTP503Acc: {}", errorHTTP503Acc.value());
				logger.info("errorHTTP525Acc: {}", errorHTTP525Acc.value());
				logger.info("errorHTTPGenericAcc: {}", errorHTTPGenericAcc.value());
				logger.info("unknowHostAcc: {}", unknowHostAcc.value());
			});

	}

	public static boolean isModified(String orcidId, String modifiedDateValue, String lastUpdateValue) {
		LocalDate modifiedDate = null;
		LocalDate lastUpdate = null;
		try {
			modifiedDate = LocalDate.parse(modifiedDateValue, SparkDownloadOrcidWorks.ORCID_XML_DATETIMEFORMATTER);
			if (lastUpdateValue.length() != 19) {
				lastUpdateValue = lastUpdateValue.substring(0, 19);
			}
			lastUpdate = LocalDate
				.parse(lastUpdateValue, SparkDownloadOrcidWorks.LAMBDA_FILE_DATE_FORMATTER);
		} catch (Exception e) {
			logger.info("[" + orcidId + "] Parsing date: ", e.getMessage());
			throw new RuntimeException("[" + orcidId + "] Parsing date: " + e.getMessage());
		}
		return modifiedDate.isAfter(lastUpdate);
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
