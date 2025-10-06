package eu.dnetlib.dhp.sx.provision;


import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.index.es.ConvertJSONWithId;
import eu.dnetlib.dhp.index.es.ESFeeder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class SparkIndexCollectionES8 {
    private static final Logger log = LoggerFactory.getLogger(SparkIndexCollectionES8.class);
    private final FileSystem fileSystem;

    public SparkIndexCollectionES8(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                Objects
                                        .requireNonNull(
                                                SparkIndexCollectionES8.class
                                                        .getResourceAsStream(
                                                                "/eu/dnetlib/dhp/sx/provision/scholexplorer_parameter.json"))));
        argumentParser.parseArgument(args);

        final String hdfsuri = argumentParser.get("namenode");
        log.info("hdfsURI is {}", hdfsuri);

        final String sourcePath = argumentParser.get("sourcePath");
        log.info("sourcePath is {}", sourcePath);

        final String index = argumentParser.get("index");
        log.info("index is {}", index);

        final String indexHost = argumentParser.get("indexHost");
        log.info("indexHost is {}", indexHost);

        final String tc = argumentParser.get("threadCount");
        log.info("threadCount is {}", tc);

        int threadCount = 10;

        if (tc != null && !tc.isEmpty()) {
            try {
                threadCount = Integer.parseInt(tc);
            } catch (NumberFormatException e) {
                log.warn("Invalid thread count provided, using default: {}", threadCount);
            }
        }

        final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));

        new SparkIndexCollectionES8(fileSystem).run(sourcePath, index, indexHost,threadCount);

    }

    public void run(final String sourcePath, final String index, final String indexHost, int threadCount)
            throws IOException {
        RemoteIterator<LocatedFileStatus> ls = fileSystem.listFiles(new Path(sourcePath), false);
        List<Path> files = new java.util.ArrayList<>();
        while (ls.hasNext()) {
            LocatedFileStatus current = ls.next();
            if (current.getPath().getName().endsWith(".gz")) {
                files.add(current.getPath());
            }
        }

        try (ESFeeder feeder = new ESFeeder(indexHost)) {
            Function<String, BulkOperation> converter = index.contains("summary")
                    ? new ConvertScholixResourceToES(index)
                    : new ConvertJSONWithId("\"identifier\":\"((\\d|\\w)*)\"", index);
            feeder.parallelBulkIndex(files, threadCount, fileSystem, converter);
       } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
