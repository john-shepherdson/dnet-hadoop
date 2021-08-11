
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.model.DownloadedRecordData;
import eu.dnetlib.doiboost.orcid.util.HDFSUtil;
import scala.Tuple2;

public class SparkDownloadOrcidAuthors {

	static Logger logger = LoggerFactory.getLogger(SparkDownloadOrcidAuthors.class);
	static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkDownloadOrcidAuthors.class
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
		logger.info("outputPath: {}", outputPath);
		final String token = parser.get("token");
		final String lambdaFileName = parser.get("lambdaFileName");
		logger.info("lambdaFileName: {}", lambdaFileName);
		final String hdfsServerUri = parser.get("hdfsServerUri");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				String lastUpdate = HDFSUtil.readFromTextFile(hdfsServerUri, workingPath, "last_update.txt");
				logger.info("lastUpdate: {}", lastUpdate);
				if (StringUtils.isBlank(lastUpdate)) {
					throw new FileNotFoundException("last update info not found");
				}
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				LongAccumulator parsedRecordsAcc = spark.sparkContext().longAccumulator("parsed_records");
				LongAccumulator modifiedRecordsAcc = spark.sparkContext().longAccumulator("to_download_records");
				LongAccumulator downloadedRecordsAcc = spark.sparkContext().longAccumulator("downloaded_records");
				LongAccumulator errorHTTP403Acc = spark.sparkContext().longAccumulator("error_HTTP_403");
				LongAccumulator errorHTTP404Acc = spark.sparkContext().longAccumulator("error_HTTP_404");
				LongAccumulator errorHTTP409Acc = spark.sparkContext().longAccumulator("error_HTTP_409");
				LongAccumulator errorHTTP503Acc = spark.sparkContext().longAccumulator("error_HTTP_503");
				LongAccumulator errorHTTP525Acc = spark.sparkContext().longAccumulator("error_HTTP_525");
				LongAccumulator errorHTTPGenericAcc = spark.sparkContext().longAccumulator("error_HTTP_Generic");

				logger.info("Retrieving data from lamda sequence file");
				JavaPairRDD<Text, Text> lamdaFileRDD = sc
					.sequenceFile(workingPath + lambdaFileName, Text.class, Text.class);
				final long lamdaFileRDDCount = lamdaFileRDD.count();
				logger.info("Data retrieved: {}", lamdaFileRDDCount);

				Function<Tuple2<Text, Text>, Boolean> isModifiedAfterFilter = data -> {
					String orcidId = data._1().toString();
					String lastModifiedDate = data._2().toString();
					parsedRecordsAcc.add(1);
					if (isModified(orcidId, lastModifiedDate, lastUpdate)) {
						modifiedRecordsAcc.add(1);
						return true;
					}
					return false;
				};

				Function<Tuple2<Text, Text>, Tuple2<String, String>> downloadRecordFn = data -> {
					String orcidId = data._1().toString();
					String lastModifiedDate = data._2().toString();
					final DownloadedRecordData downloaded = new DownloadedRecordData();
					downloaded.setOrcidId(orcidId);
					downloaded.setLastModifiedDate(lastModifiedDate);
					CloseableHttpClient client = HttpClients.createDefault();
					HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
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

				sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");

				logger.info("Start execution ...");
				JavaPairRDD<Text, Text> authorsModifiedRDD = lamdaFileRDD.filter(isModifiedAfterFilter);
				long authorsModifiedCount = authorsModifiedRDD.count();
				logger.info("Authors modified count: {}", authorsModifiedCount);

				logger.info("Start downloading ...");

				final JavaPairRDD<Text, Text> pairRDD = authorsModifiedRDD
					.repartition(100)
					.map(downloadRecordFn)
					.mapToPair(t -> new Tuple2<>(new Text(t._1()), new Text(t._2())));

				saveAsSequenceFile(workingPath, outputPath, sc, pairRDD);

				logger.info("parsedRecordsAcc: {}", parsedRecordsAcc.value());
				logger.info("modifiedRecordsAcc: {}", modifiedRecordsAcc.value());
				logger.info("downloadedRecordsAcc: {}", downloadedRecordsAcc.value());
				logger.info("errorHTTP403Acc: {}", errorHTTP403Acc.value());
				logger.info("errorHTTP404Acc: {}", errorHTTP404Acc.value());
				logger.info("errorHTTP409Acc: {}", errorHTTP409Acc.value());
				logger.info("errorHTTP503Acc: {}", errorHTTP503Acc.value());
				logger.info("errorHTTP525Acc: {}", errorHTTP525Acc.value());
				logger.info("errorHTTPGenericAcc: {}", errorHTTPGenericAcc.value());
			});

	}

	private static void saveAsSequenceFile(String workingPath, String outputPath, JavaSparkContext sc,
		JavaPairRDD<Text, Text> pairRDD) {
		pairRDD
			.saveAsNewAPIHadoopFile(
				workingPath.concat(outputPath),
				Text.class,
				Text.class,
				SequenceFileOutputFormat.class,
				sc.hadoopConfiguration());
	}

	public static boolean isModified(String orcidId, String modifiedDate, String lastUpdate) {
		Date modifiedDateDt;
		Date lastUpdateDt;
		String lastUpdateRedux = "";
		try {
			if (modifiedDate.equals("last_modified")) {
				return false;
			}
			if (modifiedDate.length() != 19) {
				modifiedDate = modifiedDate.substring(0, 19);
			}
			if (lastUpdate.length() != 19) {
				lastUpdateRedux = lastUpdate.substring(0, 19);
			} else {
				lastUpdateRedux = lastUpdate;
			}
			modifiedDateDt = new SimpleDateFormat(DATE_FORMAT).parse(modifiedDate);
			lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdateRedux);
		} catch (Exception e) {
			throw new RuntimeException("[" + orcidId + "] modifiedDate <" + modifiedDate + "> lastUpdate <" + lastUpdate
				+ "> Parsing date: " + e.getMessage());
		}
		return modifiedDateDt.after(lastUpdateDt);
	}
}
