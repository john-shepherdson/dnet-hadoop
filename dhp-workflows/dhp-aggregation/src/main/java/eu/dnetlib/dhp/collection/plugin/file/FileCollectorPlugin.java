package eu.dnetlib.dhp.collection.plugin.file;

import eu.dnetlib.dhp.common.collection.CollectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;

public class FileCollectorPlugin extends AbstractSplittedRecordPlugin {

    private static final Logger log = LoggerFactory.getLogger(FileCollectorPlugin.class);

    @Override
    protected BufferedInputStream getBufferedInputStream(final String baseUrl) throws CollectorException {

        log.info("baseUrl: {}", baseUrl);

        try {
            return new BufferedInputStream(new FileInputStream(baseUrl));
        } catch (Exception e) {
            throw new CollectorException("Error reading file " + baseUrl, e);
        }
    }
}