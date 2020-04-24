package eu.dnetlib.doiboost.orcid;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

public class OrcidAuthorsDOIsDataGen extends OrcidDSManager {

    private String activitiesFileNameTarGz;
    private String outputAuthorsDOIsPath;

    public static void main(String[] args) throws IOException, Exception {
        OrcidAuthorsDOIsDataGen orcidAuthorsDOIsDataGen = new OrcidAuthorsDOIsDataGen();
        orcidAuthorsDOIsDataGen.loadArgs(args);
        orcidAuthorsDOIsDataGen.generateAuthorsDOIsData();
    }

    public void generateAuthorsDOIsData() throws Exception {
        Configuration conf = initConfigurationObject();
        FileSystem fs = initFileSystemObject(conf);
        String tarGzUri =
                hdfsServerUri.concat(hdfsOrcidDefaultPath).concat(activitiesFileNameTarGz);
        Path outputPath =
                new Path(
                        hdfsServerUri
                                .concat(hdfsOrcidDefaultPath)
                                .concat(outputAuthorsDOIsPath)
                                .concat("authors_dois.seq"));
        ActivitiesDecompressor.parseGzActivities(conf, tarGzUri, outputPath);
    }

    private void loadArgs(String[] args) throws IOException, Exception {
        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                OrcidAuthorsDOIsDataGen.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/doiboost/create_orcid_authors_dois_data.json")));
        parser.parseArgument(args);

        hdfsServerUri = parser.get("hdfsServerUri");
        Log.info("HDFS URI: " + hdfsServerUri);
        hdfsOrcidDefaultPath = parser.get("hdfsOrcidDefaultPath");
        Log.info("Default Path: " + hdfsOrcidDefaultPath);
        activitiesFileNameTarGz = parser.get("activitiesFileNameTarGz");
        Log.info("Activities File Name: " + activitiesFileNameTarGz);
        outputAuthorsDOIsPath = parser.get("outputAuthorsDOIsPath");
        Log.info("Output Authors DOIs Data: " + outputAuthorsDOIsPath);
    }
}
