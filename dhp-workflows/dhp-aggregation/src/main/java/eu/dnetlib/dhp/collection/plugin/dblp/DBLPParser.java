
package eu.dnetlib.dhp.collection.plugin.dblp;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBLPParser implements Iterator<String> {

	private final XMLStreamReader reader;
	private final Logger logger = LoggerFactory.getLogger(DBLPParser.class);

	String currentElement = null;
	StringBuilder currentElementContent = new StringBuilder();
	String nextData = null;

	public DBLPParser(InputStream gzipStream) throws Exception {

		XMLInputFactory xif = XMLInputFactory.newFactory();
		xif.setProperty("com.ctc.wstx.maxEntityCount", 50000000);
		xif.setXMLResolver((s, s1, s2, s3) -> {
			if (s1.contains("dblp.dtd")) {
				return getClass().getResourceAsStream("dblp.dtd");
			}

			return null;
		});
		reader = xif.createXMLStreamReader(gzipStream);
		nextData = nextElement();

	}

	private String nextElement() {
		try {
			if (!reader.hasNext())
				return null;
			while (reader.hasNext()) {
				int event = reader.next();
				switch (event) {
					case XMLStreamReader.START_ELEMENT:
						if (!reader.getLocalName().equalsIgnoreCase("dblp")) {
							if (currentElement == null)
								currentElement = reader.getLocalName();
							extractRootNode(reader, currentElementContent);
						}
						break;
					case XMLStreamReader.CHARACTERS:
						if (!reader.getText().isEmpty()) {
							currentElementContent.append(StringEscapeUtils.escapeXml11(reader.getText()));
						}
						break;
					case XMLStreamReader.END_ELEMENT:
						currentElementContent.append("</" + reader.getLocalName() + ">");
						if (reader.getLocalName().equalsIgnoreCase(currentElement)) {

							String data = currentElementContent.toString();
							currentElement = null;
							currentElementContent.delete(0, currentElementContent.length());
							return data;

						}
						break;

				}
			}
		} catch (Throwable e) {
			logger.error(currentElement);
			logger.error(currentElementContent.toString());

			logger.error("Error parsing XML {}", e.getMessage());
			throw new RuntimeException(e);
		}
		return null;
	}

	public void extractRootNode(XMLStreamReader reader, StringBuilder currentElementContent) {
		currentElementContent.append("<").append(reader.getLocalName());

		// Print attributes of the current element
		int attributeCount = reader.getAttributeCount();
		for (int i = 0; i < attributeCount; i++) {
			String attributeName = reader.getAttributeLocalName(i);
			String attributeValue = reader.getAttributeValue(i);
			currentElementContent.append(" ").append(attributeName).append("=\"").append(attributeValue).append("\"");
		}

		currentElementContent.append(">");
	}

	@Override
	public boolean hasNext() {
		return nextData != null;
	}

	@Override
	public String next() {

		String data = nextData;
		nextData = nextElement();
		return data;
	}
}
