package eu.dnetlib.doiboost.orcid;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

public class OrcidDSManager {

    protected String hdfsServerUri;
    protected String hdfsOrcidDefaultPath;
    private String summariesFileNameTarGz;
    private String outputAuthorsPath;

    public static void main(String[] args) throws IOException, Exception {
        OrcidDSManager orcidDSManager = new OrcidDSManager();
        orcidDSManager.loadArgs(args);
        orcidDSManager.generateAuthors();
    }

    public void generateAuthors() throws Exception {
        Configuration conf = initConfigurationObject();
        FileSystem fs = initFileSystemObject(conf);
        String tarGzUri = hdfsServerUri.concat(hdfsOrcidDefaultPath).concat(summariesFileNameTarGz);
        Path outputPath =
                new Path(
                        hdfsServerUri
                                .concat(hdfsOrcidDefaultPath)
                                .concat(outputAuthorsPath)
                                .concat("authors.seq"));
        SummariesDecompressor.parseGzSummaries(conf, tarGzUri, outputPath);
    }

    protected Configuration initConfigurationObject() {
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsServerUri.concat(hdfsOrcidDefaultPath));
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return conf;
    }

    protected FileSystem initFileSystemObject(Configuration conf) {
        // Get the filesystem - HDFS
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(hdfsServerUri.concat(hdfsOrcidDefaultPath)), conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return fs;
    }

    private void loadArgs(String[] args) throws IOException, Exception {
        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                OrcidDSManager.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/doiboost/create_orcid_authors_data.json")));
        parser.parseArgument(args);

        hdfsServerUri = parser.get("hdfsServerUri");
        Log.info("HDFS URI: " + hdfsServerUri);
        hdfsOrcidDefaultPath = parser.get("hdfsOrcidDefaultPath");
        Log.info("Default Path: " + hdfsOrcidDefaultPath);
        summariesFileNameTarGz = parser.get("summariesFileNameTarGz");
        Log.info("Summaries File Name: " + summariesFileNameTarGz);
        outputAuthorsPath = parser.get("outputAuthorsPath");
        Log.info("Output Authors Data: " + outputAuthorsPath);
    }
}
