
package eu.dnetlib.doiboost.orcid;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcidnodoi.GenOrcidAuthorWork;

public class ExtractXMLSummariesData extends OrcidDSManager {

	private String outputAuthorsPath;
	private String summariesFileNameTarGz;

	public static void main(String[] args) throws Exception {
		ExtractXMLSummariesData extractXMLSummariesData = new ExtractXMLSummariesData();
		extractXMLSummariesData.loadArgs(args);
		extractXMLSummariesData.extractAuthors();
	}

	private void loadArgs(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenOrcidAuthorWork.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_orcid_authors_from_summaries.json")));
		parser.parseArgument(args);

		hdfsServerUri = parser.get("hdfsServerUri");
		Log.info("HDFS URI: " + hdfsServerUri);
		workingPath = parser.get("workingPath");
		Log.info("Working Path: " + workingPath);
		summariesFileNameTarGz = parser.get("summariesFileNameTarGz");
		Log.info("Summaries File Name: " + summariesFileNameTarGz);
		outputAuthorsPath = parser.get("outputAuthorsPath");
		Log.info("Output Authors Data: " + outputAuthorsPath);
	}

	public void extractAuthors() throws Exception {
		Configuration conf = initConfigurationObject();
		FileSystem fs = initFileSystemObject(conf);
		String tarGzUri = hdfsServerUri.concat(workingPath).concat(summariesFileNameTarGz);
		Path outputPath = new Path(
			hdfsServerUri
				.concat(workingPath)
				.concat(outputAuthorsPath)
				.concat("xml_authors.seq"));
		SummariesDecompressor.extractXML(conf, tarGzUri, outputPath);
	}
}
