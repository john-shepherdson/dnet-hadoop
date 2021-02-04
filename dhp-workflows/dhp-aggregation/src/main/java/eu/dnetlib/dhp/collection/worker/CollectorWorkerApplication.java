
package eu.dnetlib.dhp.collection.worker;

import static eu.dnetlib.dhp.aggregation.common.AggregationConstants.*;
import static eu.dnetlib.dhp.aggregation.common.AggregationUtility.*;
import static eu.dnetlib.dhp.application.ApplicationUtils.*;

import java.io.IOException;

import eu.dnetlib.dhp.message.Message;
import eu.dnetlib.dhp.message.MessageSender;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.aggregation.common.AggregationUtility;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginErrorLogList;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;
import eu.dnetlib.dhp.collector.worker.model.ApiDescriptor;

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

	public static final String COLLECTOR_WORKER_ERRORS = "collectorWorker-errors";

	/**
	 * @param args
	 */
	public static void main(final String[] args) throws ParseException, IOException, CollectorException {

		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CollectorWorker.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/collection/collector_parameter.json")));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String apiDescriptor = argumentParser.get("apidescriptor");
		log.info("apiDescriptor is {}", apiDescriptor);

		final String mdStoreVersion = argumentParser.get("mdStoreVersion");
		log.info("mdStoreVersion is {}", mdStoreVersion);

		final String dnetMessageManagerURL = argumentParser.get("dnetMessageManagerURL");
		log.info("dnetMessageManagerURL is {}", dnetMessageManagerURL);

		final String workflowId = argumentParser.get("workflowId");
		log.info("workflowId is {}", workflowId);

		final MessageSender ms = new MessageSender(dnetMessageManagerURL,workflowId);

		final MDStoreVersion currentVersion = MAPPER.readValue(mdStoreVersion, MDStoreVersion.class);
		final String hdfsPath = currentVersion.getHdfsPath() + SEQUENCE_FILE_NAME;
		log.info("hdfs path is {}", hdfsPath);

		final ApiDescriptor api = MAPPER.readValue(apiDescriptor, ApiDescriptor.class);

		final CollectorWorker worker = new CollectorWorker(api, hdfsuri, hdfsPath, ms);
		CollectorPluginErrorLogList errors = worker.collect();

		populateOOZIEEnv(COLLECTOR_WORKER_ERRORS, errors.toString());

	}



}
