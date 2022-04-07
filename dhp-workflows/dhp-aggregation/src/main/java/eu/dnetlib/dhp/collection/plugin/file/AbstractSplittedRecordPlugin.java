package eu.dnetlib.dhp.collection.plugin.file;

import java.io.BufferedInputStream;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.utils.XMLIterator;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSplittedRecordPlugin implements CollectorPlugin {

    private static final Logger log = LoggerFactory.getLogger(AbstractSplittedRecordPlugin.class);

    public static final String SPLIT_ON_ELEMENT = "splitOnElement";

    @Override
    public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {

        final String baseUrl = Optional
                .ofNullable(api.getBaseUrl())
                .orElseThrow( () -> new CollectorException("missing baseUrl, required by the AbstractSplittedRecordPlugin"));

        log.info("baseUrl: {}", baseUrl);

        final String splitOnElement = Optional
                .ofNullable(api.getParams().get(SPLIT_ON_ELEMENT))
                .orElseThrow(() -> new CollectorException(String.format("missing parameter '%s', required by the AbstractSplittedRecordPlugin", SPLIT_ON_ELEMENT)));

        log.info("splitOnElement: {}", splitOnElement);

        final BufferedInputStream bis = getBufferedInputStream(baseUrl);

        Iterator<String> xmlIterator = new XMLIterator(splitOnElement, bis);

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(xmlIterator, Spliterator.ORDERED),
                false
        );
    }

    abstract protected BufferedInputStream getBufferedInputStream(final String baseUrl) throws CollectorException;

}