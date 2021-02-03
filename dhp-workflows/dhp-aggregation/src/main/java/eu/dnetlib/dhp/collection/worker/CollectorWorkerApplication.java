
package eu.dnetlib.dhp.collection.worker;

import static eu.dnetlib.dhp.aggregation.common.AggregationConstants.*;
import static eu.dnetlib.dhp.aggregation.common.AggregationUtility.*;
import static eu.dnetlib.dhp.application.ApplicationUtils.*;

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
 * DnetCollectortWorkerApplication is the main class responsible to start the Dnet Collection into HDFS. This module
 * will be executed on the hadoop cluster and taking in input some parameters that tells it which is the right collector
 * plugin to use and where store the data into HDFS path
 *
 * @author Sandro La Bruzzo
 */
public class CollectorWorkerApplication {

	private static final Logger log = LoggerFactory.getLogger(CollectorWorkerApplication.class);

	/**
	 * @param args
	 */
	public static void main(final String[] args) throws Exception {

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

		final MDStoreVersion currentVersion = MAPPER.readValue(mdStoreVersion, MDStoreVersion.class);
		final String hdfsPath = currentVersion.getHdfsPath() + SEQUENCE_FILE_NAME;
		log.info("hdfs path is {}", hdfsPath);

		final ApiDescriptor api = MAPPER.readValue(apiDescriptor, ApiDescriptor.class);

		final CollectorWorker worker = new CollectorWorker(api, hdfsuri, hdfsPath);
		CollectorPluginErrorLogList errors = worker.collect();

		populateOOZIEEnv("collectorErrors", errors.toString());

	}

}
