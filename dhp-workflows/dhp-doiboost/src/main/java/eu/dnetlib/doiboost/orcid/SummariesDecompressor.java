package eu.dnetlib.doiboost.orcid;

import eu.dnetlib.doiboost.orcid.json.JsonWriter;
import eu.dnetlib.doiboost.orcid.model.AuthorData;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

public class SummariesDecompressor {
	
	private static final Logger logger = Logger.getLogger(SummariesDecompressor.class);
    
    public static void parseGzSummaries(Configuration conf, String inputUri, Path outputPath) throws Exception {
        String uri = inputUri;
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if (codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }
        CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream gzipInputStream = null;
        try {
        	gzipInputStream = codec.createInputStream(fs.open(inputPath));
        	parseTarSummaries(fs, conf, gzipInputStream, outputPath);
            
        } finally {
        	logger.debug("Closing gzip stream");
            IOUtils.closeStream(gzipInputStream);
        }
    }
    
    private static void parseTarSummaries(FileSystem fs, Configuration conf, InputStream gzipInputStream, Path outputPath) {
    	int counter = 0;
    	int nameFound = 0;
    	int surnameFound = 0;
    	int creditNameFound = 0;
    	int errorFromOrcidFound = 0;
    	int xmlParserErrorFound = 0;
    	try (TarArchiveInputStream tais = new TarArchiveInputStream(gzipInputStream)) {
    		TarArchiveEntry entry = null;
    		
    		try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
	                SequenceFile.Writer.file(outputPath), SequenceFile.Writer.keyClass(Text.class),
	                SequenceFile.Writer.valueClass(Text.class))) {
    			
			    while ((entry = tais.getNextTarEntry()) != null) {
			        String filename = entry.getName();
			        if (entry.isDirectory()) {
			        	logger.debug("Directory entry name: "+entry.getName());
			        } else {
			        	logger.debug("XML record entry name: "+entry.getName());
			        	counter++;
			        	BufferedReader br = new BufferedReader(new InputStreamReader(tais)); // Read directly from tarInput
			            String line;
			            StringBuffer buffer = new StringBuffer();
			            while ((line = br.readLine()) != null) {
			                buffer.append(line);
			            }
			        	try (ByteArrayInputStream bais = new ByteArrayInputStream(buffer.toString().getBytes())) {
							AuthorData authorData = XMLRecordParser.parse(bais);
							if (authorData!=null) {
								if (authorData.getErrorCode()!=null) {
									errorFromOrcidFound+=1;
									logger.debug("error from Orcid with code "+authorData.getErrorCode()+" for oid "+entry.getName());
									continue;
								}
								String jsonData = JsonWriter.create(authorData);
								logger.debug("oid: "+authorData.getOid() + " data: "+jsonData);
								
					            final Text key = new Text(authorData.getOid());
					            final Text value = new Text(jsonData);
					            
					            try {
	                                writer.append(key, value);
	                            } catch (IOException e) {
	                            	logger.error("Writing to sequence file: "+e.getMessage());
	                            	e.printStackTrace();
	                                throw new RuntimeException(e);
	                            }
						          
					            if (authorData.getName()!=null) {
					            	nameFound+=1;
					            }
					            if (authorData.getSurname()!=null) {
					            	surnameFound+=1;
					            }
					            if (authorData.getCreditName()!=null) {
					            	creditNameFound+=1;
					            }
					            
						        }
							else {
								logger.error("Data not retrievable ["+entry.getName()+"] "+buffer.toString());
								xmlParserErrorFound+=1;
							}
								
						} catch (XPathExpressionException | ParserConfigurationException | SAXException e) {
							logger.error("Parsing record from tar archive: "+e.getMessage());
							e.printStackTrace();
						}
			        }
			        
			        if ((counter % 100000) == 0) {
			        	logger.info("Current xml records parsed: "+counter);
			        }
			    }
    		}
		} catch (IOException e) {
			logger.error("Parsing record from gzip archive: "+e.getMessage());
			throw new RuntimeException(e);
		}
    	logger.info("Summaries parse completed");
    	logger.info("Total XML records parsed: "+counter);
    	logger.info("Name found: "+nameFound);
    	logger.info("Surname found: "+surnameFound);
    	logger.info("Credit name found: "+creditNameFound);
    	logger.info("Error from Orcid found: "+errorFromOrcidFound);
    	logger.info("Error parsing xml record found: "+xmlParserErrorFound);
    }
}