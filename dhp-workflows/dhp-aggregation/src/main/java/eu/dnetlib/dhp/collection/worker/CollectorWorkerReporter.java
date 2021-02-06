
package eu.dnetlib.dhp.collection.worker;

import static eu.dnetlib.dhp.aggregation.common.AggregationConstants.REPORT_FILE_NAME;
import static eu.dnetlib.dhp.utils.DHPUtils.*;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

/**
 * CollectorWorkerReporter
 */
public class CollectorWorkerReporter {

	private static final Logger log = LoggerFactory.getLogger(CollectorWorkerReporter.class);

	/**
	 * @param args
	 */
	public static void main(final String[] args) throws IOException, ParseException, CollectorException {

		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CollectorWorker.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/collection/collector_reporter_input_parameter.json")));
		argumentParser.parseArgument(args);

		final String nameNode = argumentParser.get("namenode");
		log.info("nameNode is {}", nameNode);

		final String mdStoreVersion = argumentParser.get("mdStoreVersion");
		log.info("mdStoreVersion is {}", mdStoreVersion);

		final MDStoreVersion currentVersion = MAPPER.readValue(mdStoreVersion, MDStoreVersion.class);

		final String reportPath = currentVersion.getHdfsPath() + REPORT_FILE_NAME;
		log.info("report path is {}", reportPath);

		final Configuration conf = getHadoopConfiguration(nameNode);
		CollectorPluginReport report = readHdfsFileAs(conf, reportPath, CollectorPluginReport.class);
		if (Objects.isNull(report)) {
			throw new CollectorException("collection report is NULL");
		}
		log.info("report success: {}, size: {}", report.isSuccess(), report.size());
		report.forEach((k, v) -> log.info("{} - {}", k, v));
		if (!report.isSuccess()) {
			throw new CollectorException("collection report indicates a failure");
		}
	}

}
