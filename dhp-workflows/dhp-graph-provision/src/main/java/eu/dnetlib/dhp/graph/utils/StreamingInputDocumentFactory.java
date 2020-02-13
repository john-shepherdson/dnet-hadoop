package eu.dnetlib.dhp.graph.utils;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.xml.stream.*;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.Lists;
import org.apache.solr.common.SolrInputDocument;

/**
 * Optimized version of the document parser, drop in replacement of InputDocumentFactory.
 *
 * <p>
 * Faster because:
 * </p>
 * <ul>
 * <li>Doesn't create a DOM for the full document</li>
 * <li>Doesn't execute xpaths agains the DOM</li>
 * <li>Quickly serialize the 'result' element directly in a string.</li>
 * <li>Uses less memory: less pressure on GC and allows more threads to process this in parallel</li>
 * </ul>
 *
 * <p>
 * This class is fully reentrant and can be invoked in parallel.
 * </p>
 *
 * @author claudio
 *
 */
public class StreamingInputDocumentFactory {

    private static final String INDEX_FIELD_PREFIX = "__";

    private static final String DS_VERSION = INDEX_FIELD_PREFIX + "dsversion";

    private static final String DS_ID = INDEX_FIELD_PREFIX + "dsid";

    private static final String RESULT = "result";

    private static final String INDEX_RESULT = INDEX_FIELD_PREFIX + RESULT;

    private static final String INDEX_RECORD_ID = INDEX_FIELD_PREFIX + "indexrecordidentifier";

    private static final String outFormat = new String("yyyy-MM-dd'T'hh:mm:ss'Z'");

    private final static List<String> dateFormats = Arrays.asList("yyyy-MM-dd'T'hh:mm:ss", "yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy");

    private static final String DEFAULTDNETRESULT = "dnetResult";

    private static final String TARGETFIELDS = "targetFields";

    private static final String INDEX_RECORD_ID_ELEMENT = "indexRecordIdentifier";

    private static final String ROOT_ELEMENT = "indexRecord";

    private static final int MAX_FIELD_LENGTH = 25000;

    private ThreadLocal<XMLInputFactory> inputFactory = ThreadLocal.withInitial(() -> XMLInputFactory.newInstance());

    private ThreadLocal<XMLOutputFactory> outputFactory = ThreadLocal.withInitial(() -> XMLOutputFactory.newInstance());

    private ThreadLocal<XMLEventFactory> eventFactory = ThreadLocal.withInitial(() -> XMLEventFactory.newInstance());

    private String version;

    private String dsId;

    private String resultName = DEFAULTDNETRESULT;

    public StreamingInputDocumentFactory(final String version, final String dsId) {
        this(version, dsId, DEFAULTDNETRESULT);
    }

    public StreamingInputDocumentFactory(final String version, final String dsId, final String resultName) {
        this.version = version;
        this.dsId = dsId;
        this.resultName = resultName;
    }

    public SolrInputDocument parseDocument(final String inputDocument) {

        final StringWriter results = new StringWriter();
        final List<Namespace> nsList = Lists.newLinkedList();
        try {

            XMLEventReader parser = inputFactory.get().createXMLEventReader(new StringReader(inputDocument));

            final SolrInputDocument indexDocument = new SolrInputDocument(new HashMap<>());

            while (parser.hasNext()) {
                final XMLEvent event = parser.nextEvent();
                if ((event != null) && event.isStartElement()) {
                    final String localName = event.asStartElement().getName().getLocalPart();

                    if (ROOT_ELEMENT.equals(localName)) {
                        nsList.addAll(getNamespaces(event));
                    } else if (INDEX_RECORD_ID_ELEMENT.equals(localName)) {
                        final XMLEvent text = parser.nextEvent();
                        String recordId = getText(text);
                        indexDocument.addField(INDEX_RECORD_ID, recordId);
                    } else if (TARGETFIELDS.equals(localName)) {
                        parseTargetFields(indexDocument, parser);
                    } else if (resultName.equals(localName)) {
                        copyResult(indexDocument, results, parser, nsList, resultName);
                    }
                }
            }

            if (version != null) {
                indexDocument.addField(DS_VERSION, version);
            }

            if (dsId != null) {
                indexDocument.addField(DS_ID, dsId);
            }

            if (!indexDocument.containsKey(INDEX_RECORD_ID)) {
                indexDocument.clear();
                System.err.println("missing indexrecord id:\n" + inputDocument);
            }

            return indexDocument;
        } catch (XMLStreamException e) {
            return new SolrInputDocument();
        }
    }

    private List<Namespace> getNamespaces(final XMLEvent event) {
        final List<Namespace> res = Lists.newLinkedList();
        @SuppressWarnings("unchecked")
        Iterator<Namespace> nsIter = event.asStartElement().getNamespaces();
        while (nsIter.hasNext()) {
            Namespace ns = nsIter.next();
            res.add(ns);
        }
        return res;
    }

    /**
     * Parse the targetFields block and add fields to the solr document.
     *
     * @param indexDocument
     * @param parser
     * @throws XMLStreamException
     */
    protected void parseTargetFields(final SolrInputDocument indexDocument, final XMLEventReader parser) throws XMLStreamException {

        boolean hasFields = false;

        while (parser.hasNext()) {
            final XMLEvent targetEvent = parser.nextEvent();
            if (targetEvent.isEndElement() && targetEvent.asEndElement().getName().getLocalPart().equals(TARGETFIELDS)) {
                break;
            }

            if (targetEvent.isStartElement()) {
                final String fieldName = targetEvent.asStartElement().getName().getLocalPart();
                final XMLEvent text = parser.nextEvent();

                String data = getText(text);

                addField(indexDocument, fieldName, data);
                hasFields = true;
            }
        }

        if (!hasFields) {
            indexDocument.clear();
        }
    }

    /**
     * Copy the /indexRecord/result element and children, preserving namespace declarations etc.
     *
     * @param indexDocument
     * @param results
     * @param parser
     * @param nsList
     * @throws XMLStreamException
     */
    protected void copyResult(final SolrInputDocument indexDocument,
                              final StringWriter results,
                              final XMLEventReader parser,
                              final List<Namespace> nsList,
                              final String dnetResult) throws XMLStreamException {
        final XMLEventWriter writer = outputFactory.get().createXMLEventWriter(results);

        for (Namespace ns : nsList) {
            eventFactory.get().createNamespace(ns.getPrefix(), ns.getNamespaceURI());
        }

        StartElement newRecord = eventFactory.get().createStartElement("", null, RESULT, null, nsList.iterator());

        // new root record
        writer.add(newRecord);

        // copy the rest as it is
        while (parser.hasNext()) {
            final XMLEvent resultEvent = parser.nextEvent();

            // TODO: replace with depth tracking instead of close tag tracking.
            if (resultEvent.isEndElement() && resultEvent.asEndElement().getName().getLocalPart().equals(dnetResult)) {
                writer.add(eventFactory.get().createEndElement("", null, RESULT));
                break;
            }

            writer.add(resultEvent);
        }
        writer.close();
        indexDocument.addField(INDEX_RESULT, results.toString());
    }

    /**
     * Helper used to add a field to a solr doc. It avoids to add empy fields
     *
     * @param indexDocument
     * @param field
     * @param value
     */
    private final void addField(final SolrInputDocument indexDocument, final String field, final String value) {
        String cleaned = value.trim();
        if (!cleaned.isEmpty()) {
            // log.info("\n\n adding field " + field.toLowerCase() + " value: " + cleaned + "\n");
            indexDocument.addField(field.toLowerCase(), cleaned);
        }
    }

    /**
     * Helper used to get the string from a text element.
     *
     * @param text
     * @return the
     */
    protected final String getText(final XMLEvent text) {
        if (text.isEndElement()) // log.warn("skipping because isEndOfElement " + text.asEndElement().getName().getLocalPart());
            return "";

        final String data = text.asCharacters().getData();
        if (data != null && data.length() > MAX_FIELD_LENGTH) {
            return data.substring(0, MAX_FIELD_LENGTH);
        }

        return data;
    }

}
