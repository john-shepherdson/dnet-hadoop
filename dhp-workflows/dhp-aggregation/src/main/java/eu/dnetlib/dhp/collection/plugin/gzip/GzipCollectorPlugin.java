package eu.dnetlib.dhp.collection.plugin.gzip;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

import java.util.stream.Stream;

public class GzipCollectorPlugin implements CollectorPlugin {

    @Override
    public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {
        return Stream.empty();
    }
}
