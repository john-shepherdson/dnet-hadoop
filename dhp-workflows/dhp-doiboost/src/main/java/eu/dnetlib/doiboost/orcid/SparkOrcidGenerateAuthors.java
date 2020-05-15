
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.model.DownloadedRecordData;
import scala.Tuple2;

public class SparkOrcidGenerateAuthors {

	static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	static final String lastUpdate = "2019-09-30 00:00:00";

	public static void main(String[] args) throws IOException, Exception {
		Logger logger = LoggerFactory.getLogger(SparkOrcidGenerateAuthors.class);
		logger.info("[ SparkOrcidGenerateAuthors STARTED]");

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkOrcidGenerateAuthors.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_orcid_authors_parameters.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
		final String workingPath = parser.get("workingPath");
		logger.info("workingPath: ", workingPath);
		final String outputAuthorsPath = parser.get("outputAuthorsPath");
		logger.info("outputAuthorsPath: ", outputAuthorsPath);
		final String token = parser.get("token");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
				JavaRDD<String> lamdaFileRDD = sc.textFile(workingPath + "last_modified.csv");
				Function<String, Boolean> isModifiedAfterFilter = line -> {
					String[] values = line.split(",");
					String orcidId = values[0];
					if (isModified(orcidId, values[3])) {
						return true;
					}
					return false;
				};
				Function<String, Tuple2<String, String>> downloadRecordFunction = line -> {
					String[] values = line.split(",");
					String orcidId = values[0];
					return downloadRecord(orcidId, token);
				};

				lamdaFileRDD
					.filter(isModifiedAfterFilter)
					.map(downloadRecordFunction)
					.rdd()
					.saveAsTextFile(workingPath.concat(outputAuthorsPath));
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
			Log.warn("[" + orcidId + "] Parsing date: ", e.getMessage());
			return true;
		}
		return modifiedDateDt.after(lastUpdateDt);
	}

	private static Tuple2<String, String> downloadRecord(String orcidId, String token) {
		final DownloadedRecordData data = new DownloadedRecordData();
		data.setOrcidId(orcidId);
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet("https://api.orcid.org/v3.0/" + orcidId + "/record");
			httpGet.addHeader("Accept", "application/vnd.orcid+xml");
			httpGet.addHeader("Authorization", String.format("Bearer %s", token));
			CloseableHttpResponse response = client.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			data.setStatusCode(statusCode);
			if (statusCode != 200) {
				Log
					.warn(
						"Downloading " + orcidId + " status code: " + response.getStatusLine().getStatusCode());
				return data.toTuple2();
			}
			data
				.setCompressedData(
					ArgumentApplicationParser.compressArgument(IOUtils.toString(response.getEntity().getContent())));
		} catch (Throwable e) {
			Log.warn("Downloading " + orcidId, e.getMessage());
			data.setErrorMessage(e.getMessage());
			return data.toTuple2();
		}
		return data.toTuple2();
	}
}
