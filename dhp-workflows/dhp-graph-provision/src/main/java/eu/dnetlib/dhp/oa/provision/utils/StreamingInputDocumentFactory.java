
package eu.dnetlib.dhp.oa.provision.utils;

import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import javax.xml.stream.*;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.common.SolrInputDocument;

import com.google.common.collect.Lists;

/**
 * Optimized version of the document parser, drop in replacement of InputDocumentFactory.
 * <p>
 * Faster because:
 * <ul>
 * <li>Doesn't create a DOM for the full document
 * <li>Doesn't execute xpaths agains the DOM
 * <li>Quickly serialize the 'result' element directly in a string.
 * <li>Uses less memory: less pressure on GC and allows more threads to process this in parallel
 * </ul>
 * <p>
 * This class is fully reentrant and can be invoked in parallel.
 *
 * @author claudio
 */
public class StreamingInputDocumentFactory implements Serializable {

	private static final String INDEX_FIELD_PREFIX = "__";

	private static final String RESULT = "result";

	private static final String INDEX_RESULT = INDEX_FIELD_PREFIX + RESULT;

	private static final String INDEX_JSON_RESULT = INDEX_FIELD_PREFIX + "json";

	private static final String INDEX_RECORD_ID = INDEX_FIELD_PREFIX + "indexrecordidentifier";

	private static final String DEFAULTDNETRESULT = "dnetResult";

	private static final String TARGETFIELDS = "targetFields";

	private static final String INDEX_RECORD_ID_ELEMENT = "indexRecordIdentifier";

	private static final String ROOT_ELEMENT = "indexRecord";

	private static final int MAX_FIELD_LENGTH = 25000;

	private final ThreadLocal<XMLInputFactory> inputFactory = ThreadLocal
		.withInitial(XMLInputFactory::newInstance);

	private final ThreadLocal<XMLOutputFactory> outputFactory = ThreadLocal
		.withInitial(XMLOutputFactory::newInstance);

	private final ThreadLocal<XMLEventFactory> eventFactory = ThreadLocal
		.withInitial(XMLEventFactory::newInstance);

	private String resultName = DEFAULTDNETRESULT;

	public StreamingInputDocumentFactory() {
		this(DEFAULTDNETRESULT);
	}

	public StreamingInputDocumentFactory(final String resultName) {
		this.resultName = resultName;
	}

	public SolrInputDocument parseDocument(final String xml) {
		return parseDocument(xml, "");
	}

	public SolrInputDocument parseDocument(final String xml, final String json) {

		final StringWriter results = new StringWriter();
		final List<Namespace> nsList = Lists.newLinkedList();
		try {

			XMLEventReader parser = inputFactory.get().createXMLEventReader(new StringReader(xml));

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
						copyResult(indexDocument, json, results, parser, nsList, resultName);
					}
				}
			}

			if (!indexDocument.containsKey(INDEX_RECORD_ID)) {
				throw new IllegalStateException("cannot extract record ID from: " + xml);
			}

			return indexDocument;
		} catch (XMLStreamException e) {
			throw new IllegalStateException(e);
		} finally {
			inputFactory.remove();
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
	 * @param indexDocument the document being populated
	 * @param parser the XML parser
	 * @throws XMLStreamException when the parser cannot parse the XML
	 */
	protected void parseTargetFields(
		final SolrInputDocument indexDocument, final XMLEventReader parser)
		throws XMLStreamException {

		boolean hasFields = false;

		while (parser.hasNext()) {
			final XMLEvent targetEvent = parser.nextEvent();
			if (targetEvent.isEndElement()
				&& targetEvent.asEndElement().getName().getLocalPart().equals(TARGETFIELDS)) {
				break;
			}

			if (targetEvent.isStartElement()) {
				final String fieldName = targetEvent.asStartElement().getName().getLocalPart();
				final XMLEvent text = parser.nextEvent();

				String data = getText(text);
				if (Objects.nonNull(data)) {
					addField(indexDocument, fieldName, data);
					hasFields = true;
				}
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
	protected void copyResult(
		final SolrInputDocument indexDocument,
		final String json,
		final StringWriter results,
		final XMLEventReader parser,
		final List<Namespace> nsList,
		final String dnetResult)
		throws XMLStreamException {

		final XMLEventWriter writer = outputFactory.get().createXMLEventWriter(results);
		final XMLEventFactory xmlEventFactory = this.eventFactory.get();
		try {

			for (Namespace ns : nsList) {
				xmlEventFactory.createNamespace(ns.getPrefix(), ns.getNamespaceURI());
			}

			StartElement newRecord = xmlEventFactory.createStartElement("", null, RESULT, null, nsList.iterator());

			// new root record
			writer.add(newRecord);

			// copy the rest as it is
			while (parser.hasNext()) {
				final XMLEvent resultEvent = parser.nextEvent();

				// TODO: replace with depth tracking instead of close tag tracking.
				if (resultEvent.isEndElement()
					&& resultEvent.asEndElement().getName().getLocalPart().equals(dnetResult)) {
					writer.add(xmlEventFactory.createEndElement("", null, RESULT));
					break;
				}

				writer.add(resultEvent);
			}
			writer.close();
			indexDocument.addField(INDEX_RESULT, results.toString());
			// indexDocument.addField(INDEX_JSON_RESULT, json);
		} finally {
			outputFactory.remove();
			eventFactory.remove();
		}
	}

	/**
	 * Helper used to add a field to a solr doc, avoids adding empty fields
	 *
	 * @param indexDocument
	 * @param field
	 * @param value
	 */
	private final void addField(
		final SolrInputDocument indexDocument, final String field, final String value) {
		String cleaned = value.trim();
		if (!cleaned.isEmpty()) {
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
		if (text.isEndElement()) {
			return "";
		}

		final String data = text.asCharacters().getData();
		if (data != null && data.length() > MAX_FIELD_LENGTH) {
			return data.substring(0, MAX_FIELD_LENGTH);
		}

		return data;
	}
}
