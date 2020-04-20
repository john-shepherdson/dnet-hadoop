package eu.dnetlib.doiboost.orcid;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class OrcidDSManager {

	private static final Logger logger = Logger.getLogger(OrcidDSManager.class);
	
	private String hdfsServerUri;
	private String hadoopUsername;
    private String hdfsOrcidDefaultPath;
    private String summariesFileNameTarGz;
    private String outputAuthorsPath;
    
    public static void main(String[] args) {
    	logger.info("OrcidDSManager started");
    	OrcidDSManager orcidDSManager = new OrcidDSManager();
    	try {
			orcidDSManager.initGARRProperties();
			orcidDSManager.generateAuthors();
		} catch (Exception e) {
			logger.error("Generating authors data: "+e.getMessage());
		}
    }

    public void generateAuthors() throws Exception {
    	Configuration conf = initConfigurationObject();
    	FileSystem fs = initFileSystemObject(conf);
    	String tarGzUri = hdfsServerUri.concat(hdfsOrcidDefaultPath).concat(summariesFileNameTarGz);
    	logger.info("Started parsing "+tarGzUri);
    	Path outputPath = new Path(hdfsServerUri.concat(hdfsOrcidDefaultPath).concat(outputAuthorsPath).concat(Long.toString(System.currentTimeMillis())).concat("/authors_part"));
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
    	// Set HADOOP user
    	System.setProperty("HADOOP_USER_NAME", hadoopUsername);
    	System.setProperty("hadoop.home.dir", "/");
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
    
    private void loadProperties() throws FileNotFoundException, IOException {
        
    	Properties appProps = new Properties();
    	ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    	appProps.load(classLoader.getResourceAsStream("orciddsmanager/props/app.properties"));
    	hdfsServerUri = appProps.getProperty("hdfs.server.uri");
    	hadoopUsername = appProps.getProperty("hdfs.hadoopusername");
    	hdfsOrcidDefaultPath = appProps.getProperty("hdfs.orcid.defaultpath");
    	summariesFileNameTarGz = appProps.getProperty("hdfs.orcid.summariesfilename.tar.gz");
    	outputAuthorsPath = appProps.getProperty("hdfs.orcid.output.authorspath");
    }
    
    private void initDefaultProperties() throws FileNotFoundException, IOException {
        
    	hdfsServerUri = "hdfs://localhost:9000";
    	hadoopUsername = "enrico.ottonello";
    	hdfsOrcidDefaultPath = "/user/enrico.ottonello/orcid/";
    	summariesFileNameTarGz = "ORCID_2019_summaries.tar.gz";
    	outputAuthorsPath = "output/";
    }
    
    private void initGARRProperties() throws FileNotFoundException, IOException {
        
    	hdfsServerUri = "hdfs://hadoop-rm1.garr-pa1.d4science.org:8020";
    	hadoopUsername = "root";
    	hdfsOrcidDefaultPath = "/data/orcid_summaries/";
    	summariesFileNameTarGz = "ORCID_2019_summaries.tar.gz";
    	outputAuthorsPath = "output/";
    }
}
