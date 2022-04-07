package eu.dnetlib.dhp.collection.plugin.file;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.stream.Stream;

public class FileGZipCollectorPluginTest {

    private static final Logger log = LoggerFactory.getLogger(FileGZipCollectorPluginTest.class);

    private final ApiDescriptor api = new ApiDescriptor();
    private FileGZipCollectorPlugin plugin;

    private static final String SPLIT_ON_ELEMENT = "repository";

    @BeforeEach
    public void setUp() {

        final String gzipFile = this
                .getClass()
                .getResource("/eu/dnetlib/dhp/collection/plugin/file/gzip/opendoar.xml.gz")
                .getFile();

        api.setBaseUrl(gzipFile);

        HashMap<String, String> params = new HashMap<>();
        params.put("splitOnElement", SPLIT_ON_ELEMENT);

        api.setParams(params);

        plugin = new FileGZipCollectorPlugin();
    }

    @Test
    void test() throws CollectorException {

        final Stream<String> stream = plugin.collect(api, new AggregatorReport());

        stream.limit(10).forEach(s -> {
            Assertions.assertTrue(s.length() > 0);
            log.info(s);
        });
    }
}
