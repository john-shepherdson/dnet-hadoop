
package eu.dnetlib.dhp.swh.utils;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static eu.dnetlib.dhp.common.Constants.*;

public class SWHUtils {

	private static final Logger log = LoggerFactory.getLogger(SWHUtils.class);

	public static HttpClientParams getClientParams(ArgumentApplicationParser argumentParser) {

		final HttpClientParams clientParams = new HttpClientParams();
		clientParams
			.setMaxNumberOfRetry(
				Optional
					.ofNullable(argumentParser.get(MAX_NUMBER_OF_RETRY))
					.map(Integer::parseInt)
					.orElse(HttpClientParams._maxNumberOfRetry));
		log.info("maxNumberOfRetry is {}", clientParams.getMaxNumberOfRetry());

		clientParams
			.setRequestDelay(
				Optional
					.ofNullable(argumentParser.get(REQUEST_DELAY))
					.map(Integer::parseInt)
					.orElse(HttpClientParams._requestDelay));
		log.info("requestDelay is {}", clientParams.getRequestDelay());

		clientParams
			.setRetryDelay(
				Optional
					.ofNullable(argumentParser.get(RETRY_DELAY))
					.map(Integer::parseInt)
					.orElse(HttpClientParams._retryDelay));
		log.info("retryDelay is {}", clientParams.getRetryDelay());

		clientParams
				.setRequestMethod(
						Optional
								.ofNullable(argumentParser.get(REQUEST_METHOD))
								.orElse(HttpClientParams._requestMethod));
		log.info("requestMethod is {}", clientParams.getRequestMethod());

		return clientParams;
	}

	public static BufferedReader getFileReader(FileSystem fs, Path inputPath) throws IOException {
		FSDataInputStream inputStream = fs.open(inputPath);
		return new BufferedReader(
				new InputStreamReader(inputStream, StandardCharsets.UTF_8));
	}

	public static SequenceFile.Writer getSequenceFileWriter(FileSystem fs, String outputPath) throws IOException {
		return SequenceFile
				.createWriter(
						fs.getConf(),
						SequenceFile.Writer.file(new Path(outputPath)),
						SequenceFile.Writer.keyClass(Text.class),
						SequenceFile.Writer.valueClass(Text.class));
	}

	public static SequenceFile.Reader getSequenceFileReader(FileSystem fs, String inputPath) throws IOException {
		Path filePath = new Path(inputPath);
		SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(filePath);

		return new SequenceFile.Reader(fs.getConf(), fileOption);
	}

	public static void appendToSequenceFile(SequenceFile.Writer fw, String keyStr, String valueStr) throws IOException {
		Text key = new Text();
		key.set(keyStr);

		Text value = new Text();
		value.set(valueStr);

		fw.append(key, value);
	}
}
