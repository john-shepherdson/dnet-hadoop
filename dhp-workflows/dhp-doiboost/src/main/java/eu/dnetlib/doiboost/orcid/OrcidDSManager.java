package eu.dnetlib.doiboost.orcid;

import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class OrcidDSManager {

	private static final Logger logger = LoggerFactory.getLogger(OrcidDSManager.class);
	
	private String hdfsServerUri;
    private String hdfsOrcidDefaultPath;
    private String summariesFileNameTarGz;
    private String outputAuthorsPath;
    
    public static void main(String[] args) throws IOException, Exception {
    	logger.info("OrcidDSManager started");
    	OrcidDSManager orcidDSManager = new OrcidDSManager();
		orcidDSManager.loadArgs(args);
		orcidDSManager.generateAuthors();
    }

    public void generateAuthors() throws Exception {
    	Configuration conf = initConfigurationObject();
    	FileSystem fs = initFileSystemObject(conf);
    	String tarGzUri = hdfsServerUri.concat(hdfsOrcidDefaultPath).concat(summariesFileNameTarGz);
    	logger.info("Started parsing "+tarGzUri);
    	Path outputPath = new Path(hdfsServerUri.concat(hdfsOrcidDefaultPath).concat(outputAuthorsPath).concat(Long.toString(System.currentTimeMillis())).concat("/authors.seq"));
    	SummariesDecompressor.parseGzSummaries(conf, tarGzUri, outputPath);
    }
    
    private Configuration initConfigurationObject() {
    	// ====== Init HDFS File System Object
    	Configuration conf = new Configuration();
    	// Set FileSystem URI
    	conf.set("fs.defaultFS", hdfsServerUri.concat(hdfsOrcidDefaultPath));
    	// Because of Maven
    	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    	conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    	return conf;
    }
    
    private FileSystem initFileSystemObject(Configuration conf) {
    	//Get the filesystem - HDFS
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
    	final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(OrcidDSManager.class.getResourceAsStream("/eu/dnetlib/dhp/doiboost/create_orcid_authors_data.json")));
        parser.parseArgument(args);

        final String hdfsServerUri = parser.get("hdfsServerUri");
        logger.info("HDFS URI: "+hdfsServerUri);
        Path hdfsOrcidDefaultPath = new Path(parser.get("hdfsOrcidDefaultPath"));
        logger.info("Default Path: "+hdfsOrcidDefaultPath);
        final String summariesFileNameTarGz = parser.get("summariesFileNameTarGz");
        logger.info("Summaries File Name: "+summariesFileNameTarGz);
        final String outputAuthorsPath = parser.get("summariesFileNameTarGz");
        logger.info("Output Authors Data: "+outputAuthorsPath);
    }
}
