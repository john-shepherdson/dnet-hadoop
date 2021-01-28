
package eu.dnetlib.dhp.collection.worker;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;

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
		final String hdfsPath = argumentParser.get("hdfsPath");
		log.info("hdfsPath is {}" + hdfsPath);
		final String apiDescriptor = argumentParser.get("apidescriptor");
		log.info("apiDescriptor is {}" + apiDescriptor);

		final ObjectMapper jsonMapper = new ObjectMapper();

		final ApiDescriptor api = jsonMapper.readValue(apiDescriptor, ApiDescriptor.class);

		final CollectorWorker worker = new CollectorWorker(collectorPluginFactory, api, hdfsuri, hdfsPath);
		worker.collect();
	}
}
