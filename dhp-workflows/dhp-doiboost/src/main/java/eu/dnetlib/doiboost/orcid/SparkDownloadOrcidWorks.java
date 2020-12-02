
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.model.DownloadedRecordData;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import scala.Tuple2;

public class SparkDownloadOrcidWorks {

	static Logger logger = LoggerFactory.getLogger(SparkDownloadOrcidWorks.class);
	static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	static final String lastUpdate = "2020-09-29 00:00:00";

	public static void main(String[] args) throws IOException, Exception {

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
		logger.info("workingPath: ", workingPath);
//        final String outputPath = parser.get("outputPath");
		final String outputPath = "downloads/updated_works";
		logger.info("outputPath: ", outputPath);
		final String token = parser.get("token");
//        final String lambdaFileName = parser.get("lambdaFileName");
//        logger.info("lambdaFileName: ", lambdaFileName);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				LongAccumulator parsedRecordsAcc = spark.sparkContext().longAccumulator("parsed_records");
				LongAccumulator modifiedRecordsAcc = spark.sparkContext().longAccumulator("to_download_records");
				LongAccumulator downloadedRecordsAcc = spark.sparkContext().longAccumulator("downloaded_records");
				LongAccumulator errorHTTP403Acc = spark.sparkContext().longAccumulator("error_HTTP_403");
				LongAccumulator errorHTTP409Acc = spark.sparkContext().longAccumulator("error_HTTP_409");
				LongAccumulator errorHTTP503Acc = spark.sparkContext().longAccumulator("error_HTTP_503");
				LongAccumulator errorHTTP525Acc = spark.sparkContext().longAccumulator("error_HTTP_525");
				LongAccumulator errorHTTPGenericAcc = spark.sparkContext().longAccumulator("error_HTTP_Generic");

				logger.info("Retrieving updated authors");
				JavaPairRDD<Text, Text> updatedAuthorsRDD = sc
					.sequenceFile(workingPath + "downloads/updated_authors/*", Text.class, Text.class);
				logger.info("Updated authors retrieved: " + updatedAuthorsRDD.count());

				Function<Tuple2<Text, Text>, Iterator<String>> retrieveWorkUrlFunction = data -> {
					String orcidId = data._1().toString();
					String jsonData = data._2().toString();
					List<String> orcidIdWorkId = Lists.newArrayList();
					Map<String, String> workIdLastModifiedDate = retrieveWorkIdLastModifiedDate(jsonData);
					workIdLastModifiedDate.forEach((k, v) -> {
						if (isModified(orcidId, v)) {
							orcidIdWorkId.add(orcidId.concat("/work/").concat(k));
						}
					});
					Iterator<String> iterator = orcidIdWorkId.iterator();
					return iterator;
				};

				List<Iterator<String>> toDownloadWorksRDD = updatedAuthorsRDD
					.map(retrieveWorkUrlFunction)
					.take(1000);
				sc.parallelize(toDownloadWorksRDD).saveAsTextFile(workingPath.concat("downloads/updated_works_test/"));

				Function<Tuple2<Text, Text>, Tuple2<String, String>> downloadRecordFunction = data -> {
					String orcidId = data._1().toString();
					String lastModifiedDate = data._2().toString();
					final DownloadedRecordData downloaded = new DownloadedRecordData();
					downloaded.setOrcidId(orcidId);
					downloaded.setLastModifiedDate(lastModifiedDate);
					try (CloseableHttpClient client = HttpClients.createDefault()) {
						HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/work");
						httpGet.addHeader("Accept", "application/vnd.orcid+xml");
						httpGet.addHeader("Authorization", String.format("Bearer %s", token));
						long startReq = System.currentTimeMillis();
						CloseableHttpResponse response = client.execute(httpGet);
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
								case 409:
									errorHTTP409Acc.add(1);
								case 503:
									errorHTTP503Acc.add(1);
									throw new RuntimeException("Orcid request rate limit reached (HTTP 503)");
								case 525:
									errorHTTP525Acc.add(1);
								default:
									errorHTTPGenericAcc.add(1);
									logger
										.info(
											"Downloading " + orcidId + " status code: "
												+ response.getStatusLine().getStatusCode());
							}
							return downloaded.toTuple2();
						}
						downloadedRecordsAcc.add(1);
						downloaded
							.setCompressedData(
								ArgumentApplicationParser
									.compressArgument(IOUtils.toString(response.getEntity().getContent())));
					} catch (Throwable e) {
						logger.info("Downloading " + orcidId, e.getMessage());
						downloaded.setErrorMessage(e.getMessage());
						return downloaded.toTuple2();
					}
					return downloaded.toTuple2();
				};

//                    sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");

//                    logger.info("Start downloading ...");
//                    updatedAuthorsRDD
//                            .map(downloadRecordFunction)
//                            .mapToPair(t -> new Tuple2(new Text(t._1()), new Text(t._2())))
//                            .saveAsNewAPIHadoopFile(
//                                    workingPath.concat(outputPath),
//                                    Text.class,
//                                    Text.class,
//                                    SequenceFileOutputFormat.class,
//                                    sc.hadoopConfiguration());
//                    logger.info("parsedRecordsAcc: " + parsedRecordsAcc.value().toString());
//                    logger.info("modifiedRecordsAcc: " + modifiedRecordsAcc.value().toString());
//                    logger.info("downloadedRecordsAcc: " + downloadedRecordsAcc.value().toString());
//                    logger.info("errorHTTP403Acc: " + errorHTTP403Acc.value().toString());
//                    logger.info("errorHTTP409Acc: " + errorHTTP409Acc.value().toString());
//                    logger.info("errorHTTP503Acc: " + errorHTTP503Acc.value().toString());
//                    logger.info("errorHTTP525Acc: " + errorHTTP525Acc.value().toString());
//                    logger.info("errorHTTPGenericAcc: " + errorHTTPGenericAcc.value().toString());
			});

	}

	private static boolean isModified(String orcidId, String modifiedDate) {
		Date modifiedDateDt = null;
		Date lastUpdateDt = null;
		try {
			if (modifiedDate.length() != 19) {
				modifiedDate = modifiedDate.substring(0, 19);
			}
			modifiedDateDt = new SimpleDateFormat(DATE_FORMAT).parse(modifiedDate);
			lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdate);
		} catch (Exception e) {
			logger.info("[" + orcidId + "] Parsing date: ", e.getMessage());
			return true;
		}
		return modifiedDateDt.after(lastUpdateDt);
	}

	private static Map<String, String> retrieveWorkIdLastModifiedDate(String json)
		throws XPathEvalException, NavException, XPathParseException, ParseException {
		JsonElement jElement = new JsonParser().parse(json);
		String statusCode = getJsonValue(jElement, "statusCode");
		if (statusCode.equals("200")) {
			String compressedData = getJsonValue(jElement, "compressedData");
			String authorSummary = ArgumentApplicationParser.decompressValue(compressedData);
			return XMLRecordParser.retrieveWorkIdLastModifiedDate(authorSummary.getBytes());
		}
		return new HashMap<>();
	}

	private static String getJsonValue(JsonElement jElement, String property) {
		if (jElement.getAsJsonObject().has(property)) {
			JsonElement name = null;
			name = jElement.getAsJsonObject().get(property);
			if (name != null && !name.isJsonNull()) {
				return name.getAsString();
			}
		}
		return new String("");
	}
}
