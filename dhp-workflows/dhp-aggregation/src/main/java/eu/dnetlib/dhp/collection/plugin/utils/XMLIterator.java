
package eu.dnetlib.dhp.collection.plugin.utils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class XMLIterator implements Iterator<String> {

	private static final Log log = LogFactory.getLog(XMLIterator.class);

	private ThreadLocal<XMLInputFactory> inputFactory = new ThreadLocal<XMLInputFactory>() {

		@Override
		protected XMLInputFactory initialValue() {
			return XMLInputFactory.newInstance();
		}
	};

	private ThreadLocal<XMLOutputFactory> outputFactory = new ThreadLocal<XMLOutputFactory>() {

		@Override
		protected XMLOutputFactory initialValue() {
			return XMLOutputFactory.newInstance();
		}
	};

	private ThreadLocal<XMLEventFactory> eventFactory = new ThreadLocal<XMLEventFactory>() {

		@Override
		protected XMLEventFactory initialValue() {
			return XMLEventFactory.newInstance();
		}
	};

	public static final String UTF_8 = "UTF-8";

	final XMLEventReader parser;

	private XMLEvent current = null;

	private String element;

	private List<String> elements;

	private InputStream inputStream;

	public XMLIterator(final String element, final InputStream inputStream) {
		super();
		this.element = element;
		if (element.contains(",")) {
			elements = Arrays
				.stream(element.split(","))
				.filter(StringUtils::isNoneBlank)
				.map(String::toLowerCase)
				.collect(Collectors.toList());
		}
		this.inputStream = inputStream;
		this.parser = getParser();

		try {
			this.current = findElement(parser);
		} catch (XMLStreamException e) {
			log.warn("cannot init parser position. No element found: " + element);
			current = null;
		}
	}

	@Override
	public boolean hasNext() {
		return current != null;
	}

	@Override
	public String next() {
		String result = null;
		try {
			result = copy(parser);
			current = findElement(parser);
			return result;
		} catch (XMLStreamException e) {
			throw new RuntimeException(String.format("error copying xml, built so far: '%s'", result), e);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("finally")
	private String copy(final XMLEventReader parser) throws XMLStreamException {
		final StringWriter result = new StringWriter();
		try {
			final XMLEventWriter writer = outputFactory.get().createXMLEventWriter(result);
			final StartElement start = current.asStartElement();
			final StartElement newRecord = eventFactory
				.get()
				.createStartElement(start.getName(), start.getAttributes(), start.getNamespaces());

			// new root record
			writer.add(newRecord);

			// copy the rest as it is
			while (parser.hasNext()) {
				final XMLEvent event = parser.nextEvent();

				// TODO: replace with depth tracking instead of close tag tracking.
				if (event.isEndElement() && isCheckTag(event.asEndElement().getName().getLocalPart())) {
					writer.add(event);
					break;
				}

				writer.add(event);
			}
			writer.close();
		} finally {
			return result.toString();
		}
	}

	/**
	 * Looks for the next occurrence of the splitter element.
	 *
	 * @param parser
	 * @return
	 * @throws XMLStreamException
	 */
	private XMLEvent findElement(final XMLEventReader parser) throws XMLStreamException {

		/*
		 * if (current != null && element.equals(current.asStartElement().getName().getLocalPart())) { return current; }
		 */

		XMLEvent peek = parser.peek();
		if (peek != null && peek.isStartElement()) {
			String name = peek.asStartElement().getName().getLocalPart();
			if (isCheckTag(name))
				return peek;
		}

		while (parser.hasNext()) {
			XMLEvent event = parser.nextEvent();
			if (event != null && event.isStartElement()) {
				String name = event.asStartElement().getName().getLocalPart();
				if (isCheckTag(name))
					return event;
			}
		}
		return null;
	}

	private XMLEventReader getParser() {
		try {
			XMLInputFactory xif = inputFactory.get();
			xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
			return xif.createXMLEventReader(sanitize(inputStream));
		} catch (XMLStreamException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isCheckTag(final String tagName) {
		if (elements != null) {
			final String found = elements
				.stream()
				.filter(e -> e.equalsIgnoreCase(tagName))
				.findFirst()
				.orElse(null);
			if (found != null)
				return true;
		} else {
			if (element.equalsIgnoreCase(tagName)) {
				return true;
			}
		}
		return false;
	}

	private Reader sanitize(final InputStream in) {
		final CharsetDecoder charsetDecoder = Charset.forName(UTF_8).newDecoder();
		charsetDecoder.onMalformedInput(CodingErrorAction.REPLACE);
		charsetDecoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
		return new InputStreamReader(in, charsetDecoder);
	}

}
