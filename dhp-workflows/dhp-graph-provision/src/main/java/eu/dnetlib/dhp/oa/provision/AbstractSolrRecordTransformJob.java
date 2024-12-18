/*
 * Copyright (c) 2024.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.oa.provision;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;

public abstract class AbstractSolrRecordTransformJob {

	protected static String toIndexRecord(Transformer tr, final String xmlRecord) {
		final StreamResult res = new StreamResult(new StringWriter());
		try {
			tr.transform(new StreamSource(new StringReader(xmlRecord)), res);
			return res.getWriter().toString();
		} catch (TransformerException e) {
			throw new IllegalArgumentException("XPathException on record: \n" + xmlRecord, e);
		}
	}

	/**
	 * Creates the XSLT responsible for building the index xml records.
	 *
	 * @param format Metadata format name (DMF|TMF)
	 * @param xslt xslt for building the index record transformer
	 * @param fields the list of fields
	 * @return the javax.xml.transform.Transformer
	 * @throws TransformerException could happen
	 */
	protected static String getLayoutTransformer(String format, String fields, String xslt)
		throws TransformerException {

		final Transformer layoutTransformer = SaxonTransformerFactory.newInstance(xslt);
		final StreamResult layoutToXsltXslt = new StreamResult(new StringWriter());

		layoutTransformer.setParameter("format", format);
		layoutTransformer.transform(new StreamSource(new StringReader(fields)), layoutToXsltXslt);

		return layoutToXsltXslt.getWriter().toString();
	}

}
