package eu.dnetlib.orciddsmanager.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import eu.dnetlib.orciddsmanager.model.AuthorData;


public class XMLRecordParser {

	public static AuthorData parse(ByteArrayInputStream bytesStream) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
		bytesStream.reset();
		DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
		builderFactory.setNamespaceAware(true);
		DocumentBuilder builder = builderFactory.newDocumentBuilder();
		
		Document xmlDocument = builder.parse(bytesStream);
		XPath xPath = XPathFactory.newInstance().newXPath();
		xPath.setNamespaceContext(new NamespaceContext() {
		    @Override
		    public Iterator getPrefixes(String arg0) {
		        return null;
		    }
		    @Override
		    public String getPrefix(String arg0) {
		        return null;
		    }
		    @Override
		    public String getNamespaceURI(String arg0) {
		        if ("common".equals(arg0)) {
		            return "http://www.orcid.org/ns/common";
		        }
		        else if ("person".equals(arg0)) {
		            return "http://www.orcid.org/ns/person";
		        }
		        else if ("personal-details".equals(arg0)) {
		            return "http://www.orcid.org/ns/personal-details";
		        }
		        else if ("other-name".equals(arg0)) {
		            return "http://www.orcid.org/ns/other-name";
		        }
		        else if ("record".equals(arg0)) {
		            return "http://www.orcid.org/ns/record";
		        }
		        else if ("error".equals(arg0)) {
		            return "http://www.orcid.org/ns/error";
		        }
		        return null;
		    }
		});
		
		AuthorData authorData = new AuthorData();
		String errorPath = "//error:response-code";
		String error = (String)xPath.compile(errorPath).evaluate(xmlDocument, XPathConstants.STRING);
		if (!StringUtils.isBlank(error)) {
			authorData.setErrorCode(error);
			return authorData;
		}
		String oidPath = "//record:record/@path";
		String oid = (String)xPath.compile(oidPath).evaluate(xmlDocument, XPathConstants.STRING);
		if (!StringUtils.isBlank(oid)) {
			oid = oid.substring(1);
			authorData.setOid(oid);
		}
		else {
			return null;
		}
		String namePath = "//personal-details:given-names";
		String name = (String)xPath.compile(namePath).evaluate(xmlDocument, XPathConstants.STRING);
		if (!StringUtils.isBlank(name)) {
			authorData.setName(name);
		}
		String surnamePath = "//personal-details:family-name";
		String surname = (String)xPath.compile(surnamePath).evaluate(xmlDocument, XPathConstants.STRING);
		if (!StringUtils.isBlank(surname)) {
			authorData.setSurname(surname);
		}
		String creditnamePath = "//personal-details:credit-name";
		String creditName = (String)xPath.compile(creditnamePath).evaluate(xmlDocument, XPathConstants.STRING);
		if (!StringUtils.isBlank(creditName)) {
			authorData.setCreditName(creditName);
		}
		return authorData;
	}
}
