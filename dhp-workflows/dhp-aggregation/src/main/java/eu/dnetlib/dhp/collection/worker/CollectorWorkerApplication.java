
package eu.dnetlib.dhp.collection.worker;

import static eu.dnetlib.dhp.aggregation.common.AggregationConstants.*;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
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

	private static final CollectorPluginFactory collectorPluginFactory = new CollectorPluginFactory();

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

		final ObjectMapper jsonMapper = new ObjectMapper();

		final MDStoreVersion currentVersion = jsonMapper.readValue(mdStoreVersion, MDStoreVersion.class);

		final ApiDescriptor api = jsonMapper.readValue(apiDescriptor, ApiDescriptor.class);
		final CollectorWorker worker = new CollectorWorker(collectorPluginFactory, api, hdfsuri,
			currentVersion.getHdfsPath() + SEQUENCE_FILE_NAME);
		worker.collect();

	}

}
