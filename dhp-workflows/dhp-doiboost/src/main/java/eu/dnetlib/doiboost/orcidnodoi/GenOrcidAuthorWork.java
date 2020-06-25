
package eu.dnetlib.doiboost.orcidnodoi;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.OrcidDSManager;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import java.io.IOException;

public class GenOrcidAuthorWork extends OrcidDSManager {

	private String activitiesFileNameTarGz;
	private String outputWorksPath;
	private String workingPath;

	public static void main(String[] args) throws IOException, Exception {
		GenOrcidAuthorWork genOrcidAuthorWork = new GenOrcidAuthorWork();
		genOrcidAuthorWork.loadArgs(args);
		genOrcidAuthorWork.generateAuthorsDOIsData();
	}

	public void generateAuthorsDOIsData() throws Exception {
		Configuration conf = initConfigurationObject();
		FileSystem fs = initFileSystemObject(conf);
		String tarGzUri = hdfsServerUri.concat(workingPath).concat(activitiesFileNameTarGz);
		Path outputPath = new Path(hdfsServerUri.concat(workingPath).concat(outputWorksPath));
		ActivitiesDumpReader.parseGzActivities(conf, tarGzUri, outputPath);
	}

	private void loadArgs(String[] args) throws IOException, Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenOrcidAuthorWork.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_enriched_orcid_works_parameters.json")));
		parser.parseArgument(args);

		hdfsServerUri = parser.get("hdfsServerUri");
		Log.info("HDFS URI: " + hdfsServerUri);
		workingPath = parser.get("workingPath");
		Log.info("Working Path: " + workingPath);
		activitiesFileNameTarGz = parser.get("activitiesFileNameTarGz");
		Log.info("Activities File Name: " + activitiesFileNameTarGz);
		outputWorksPath = parser.get("outputWorksPath");
		Log.info("Output Author Work Data: " + outputWorksPath);
	}
}
