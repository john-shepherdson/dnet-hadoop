package eu.dnetlib.dhp.collection.plugin.file;

import eu.dnetlib.dhp.common.collection.CollectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public class FileGZipCollectorPlugin extends AbstractSplittedRecordPlugin {

    private static final Logger log = LoggerFactory.getLogger(FileGZipCollectorPlugin.class);

    @Override
    protected BufferedInputStream getBufferedInputStream(String baseUrl) throws CollectorException {

        log.info("baseUrl: {}", baseUrl);

        try {
            GZIPInputStream stream = new GZIPInputStream(new FileInputStream(baseUrl));
            return new BufferedInputStream(stream);
        } catch (Exception e) {
            e.printStackTrace();
            throw new CollectorException(e);
        }
    }
}
