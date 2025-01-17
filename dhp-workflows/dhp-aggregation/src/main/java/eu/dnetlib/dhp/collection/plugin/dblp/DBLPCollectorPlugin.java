package eu.dnetlib.dhp.collection.plugin.dblp;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

public class DBLPCollectorPlugin implements CollectorPlugin {
    @Override
    public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {
        final String dblpURL = api.getBaseUrl();
        final String hdfsURI = api.getParams().get("hdfsURI");
        final FileSystem fileSystem = initializeFileSystem(hdfsURI);
        return doStream(fileSystem, dblpURL);
    }

    private Stream<String> doStream(FileSystem fileSystem, String dblpURL) throws CollectorException {
        try {
            CompressionCodecFactory factory = new CompressionCodecFactory(fileSystem.getConf());
            Path sourcePath = new Path(dblpURL);
            CompressionCodec codec = factory.getCodec(sourcePath);
            InputStream gis = codec.createInputStream(fileSystem.open(sourcePath));
            Iterable<String> iterable = () -> {
                try {
                    return new DBLPParser(gis);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            return StreamSupport.stream(iterable.spliterator(), false);
        } catch (Throwable e) {
            throw new CollectorException(e);
        }
    }

    public FileSystem initializeFileSystem(final String hdfsURI) {
        try {
            return FileSystem.get(getHadoopConfiguration(hdfsURI));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
