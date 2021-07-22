
package eu.dnetlib.doiboost;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.dnetlib.dhp.actionmanager.project.utils.ReadCSV;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class GetCSV {
	private static final Log log = LogFactory.getLog(eu.dnetlib.dhp.actionmanager.project.utils.ReadCSV.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GetCSV.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_unibi_issn_gold_parameters.json")));

		parser.parseArgument(args);

		final String fileURL = parser.get("fileURL");
		final String hdfsPath = parser.get("hdfsPath");
		final String hdfsNameNode = parser.get("hdfsNameNode");
		final String classForName = parser.get("classForName");

		try (final ReadCSV readCSV = new ReadCSV(hdfsPath, hdfsNameNode, fileURL, ',')) {

			log.info("Getting CSV file...");
			readCSV.execute(classForName);

		}
	}

}
