package eu.dnetlib.dhp.collection.plugin.fileGZip;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class FileGZipCollectorPlugin implements CollectorPlugin {

    private static final Logger log = LoggerFactory.getLogger(FileGZipCollectorPlugin.class);

    public static final String ENCODING = "encoding";

    @Override
    public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {

        final String baseUrl = Optional
            .ofNullable(api.getBaseUrl())
            .orElseThrow( () -> new CollectorException("missing baseUrl, required by the fileGZip collector plugin"));

        log.info("fileGZip.baseUrl: {}", baseUrl);

        final String encoding = Optional
                .ofNullable(api.getParams().get(ENCODING))
                .orElseThrow(() -> new CollectorException(String.format("missing parameter '%s', required by the fileGZip collector plugin", ENCODING)));

        log.info("fileGZip.encoding: {}", encoding);

        try {

            InputStream gzipStream = new GZIPInputStream(new FileInputStream(baseUrl));
            Reader decoder = new InputStreamReader(gzipStream, encoding);
            BufferedReader reader = new BufferedReader(decoder);

            return reader.lines();

        } catch (Exception e) {
            throw new CollectorException(e);
        }
    }
}
