
package eu.dnetlib.dhp.collection;

import static eu.dnetlib.dhp.common.Constants.*;
import static eu.dnetlib.dhp.utils.DHPUtils.*;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.aggregation.common.AggregatorReport;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.message.MessageSender;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;

/**
 * CollectorWorkerApplication is the main class responsible to start the metadata collection process, storing the outcomes
 * into HDFS. This application will be executed on the hadoop cluster, where invoked in the context of the metadata collection
 * oozie workflow, it will receive all the input parameters necessary to instantiate the specific collection plugin and the
 * relative specific configurations
 *
 * @author Sandro La Bruzzo, Claudio Atzori
 */
public class CollectorWorkerApplication {

	private static final Logger log = LoggerFactory.getLogger(CollectorWorkerApplication.class);

	private FileSystem fileSystem;

	public CollectorWorkerApplication(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args)
		throws ParseException, IOException, UnknownCollectorPluginException, CollectorException {

		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CollectorWorkerApplication.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/collection/collector_worker_input_parameter.json")));
		argumentParser.parseArgument(args);

		log.info("Java Xmx: {}m", Runtime.getRuntime().maxMemory() / (1024 * 1024));

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String apiDescriptor = argumentParser.get("apidescriptor");
		log.info("apiDescriptor is {}", apiDescriptor);

		final String mdStoreVersion = argumentParser.get("mdStoreVersion");
		log.info("mdStoreVersion is {}", mdStoreVersion);

		final String dnetMessageManagerURL = argumentParser.get(DNET_MESSAGE_MGR_URL);
		log.info("dnetMessageManagerURL is {}", dnetMessageManagerURL);

		final String workflowId = argumentParser.get("workflowId");
		log.info("workflowId is {}", workflowId);

		final HttpClientParams clientParams = getClientParams(argumentParser);

		final ApiDescriptor api = MAPPER.readValue(apiDescriptor, ApiDescriptor.class);
		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));

		new CollectorWorkerApplication(fileSystem)
			.run(mdStoreVersion, clientParams, api, dnetMessageManagerURL, workflowId);
	}

	protected void run(String mdStoreVersion, HttpClientParams clientParams, ApiDescriptor api,
		String dnetMessageManagerURL, String workflowId)
		throws IOException, CollectorException, UnknownCollectorPluginException {

		final MDStoreVersion currentVersion = MAPPER.readValue(mdStoreVersion, MDStoreVersion.class);
		final MessageSender ms = new MessageSender(dnetMessageManagerURL, workflowId);

		try (AggregatorReport report = new AggregatorReport(ms)) {
			new CollectorWorker(api, fileSystem, currentVersion, clientParams, report).collect();
		}
	}

	private static HttpClientParams getClientParams(ArgumentApplicationParser argumentParser) {
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
			.setConnectTimeOut(
				Optional
					.ofNullable(argumentParser.get(CONNECT_TIMEOUT))
					.map(Integer::parseInt)
					.orElse(HttpClientParams._connectTimeOut));
		log.info("connectTimeOut is {}", clientParams.getConnectTimeOut());

		clientParams
			.setReadTimeOut(
				Optional
					.ofNullable(argumentParser.get(READ_TIMEOUT))
					.map(Integer::parseInt)
					.orElse(HttpClientParams._readTimeOut));
		log.info("readTimeOut is {}", clientParams.getReadTimeOut());
		return clientParams;
	}

}
