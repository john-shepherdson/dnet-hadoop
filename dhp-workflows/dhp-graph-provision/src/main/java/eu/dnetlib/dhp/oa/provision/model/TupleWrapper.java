/*
 * Copyright (c) 2024.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;

public class TupleWrapper implements Serializable {

	private static final long serialVersionUID = -1418439827125577822L;
	private String xml;

	private String json;

	public TupleWrapper() {
	}

	public TupleWrapper(String xml, String json) {
		this.xml = xml;
		this.json = json;
	}

	public String getXml() {
		return xml;
	}

	public void setXml(String xml) {
		this.xml = xml;
	}

	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}
}
